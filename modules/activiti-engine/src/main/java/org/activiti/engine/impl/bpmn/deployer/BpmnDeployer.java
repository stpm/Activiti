/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.activiti.engine.impl.bpmn.deployer;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.EventDefinition;
import org.activiti.bpmn.model.FlowElement;
import org.activiti.bpmn.model.Message;
import org.activiti.bpmn.model.MessageEventDefinition;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.Signal;
import org.activiti.bpmn.model.SignalEventDefinition;
import org.activiti.bpmn.model.StartEvent;
import org.activiti.bpmn.model.TimerEventDefinition;
import org.activiti.engine.ActivitiException;
import org.activiti.engine.ProcessEngineConfiguration;
import org.activiti.engine.delegate.Expression;
import org.activiti.engine.delegate.event.ActivitiEventType;
import org.activiti.engine.delegate.event.impl.ActivitiEventBuilder;
import org.activiti.engine.impl.bpmn.parser.BpmnParse;
import org.activiti.engine.impl.bpmn.parser.BpmnParser;
import org.activiti.engine.impl.cfg.IdGenerator;
import org.activiti.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.activiti.engine.impl.cmd.CancelJobsCmd;
import org.activiti.engine.impl.cmd.DeploymentSettings;
import org.activiti.engine.impl.context.Context;
import org.activiti.engine.impl.el.ExpressionManager;
import org.activiti.engine.impl.event.MessageEventHandler;
import org.activiti.engine.impl.event.SignalEventHandler;
import org.activiti.engine.impl.interceptor.CommandContext;
import org.activiti.engine.impl.jobexecutor.TimerEventHandler;
import org.activiti.engine.impl.jobexecutor.TimerStartEventJobHandler;
import org.activiti.engine.impl.persistence.deploy.Deployer;
import org.activiti.engine.impl.persistence.deploy.DeploymentManager;
import org.activiti.engine.impl.persistence.deploy.ProcessDefinitionCacheEntry;
import org.activiti.engine.impl.persistence.deploy.ProcessDefinitionInfoCacheObject;
import org.activiti.engine.impl.persistence.entity.DeploymentEntity;
import org.activiti.engine.impl.persistence.entity.EventSubscriptionEntity;
import org.activiti.engine.impl.persistence.entity.EventSubscriptionEntityManager;
import org.activiti.engine.impl.persistence.entity.IdentityLinkEntity;
import org.activiti.engine.impl.persistence.entity.MessageEventSubscriptionEntity;
import org.activiti.engine.impl.persistence.entity.ProcessDefinitionEntity;
import org.activiti.engine.impl.persistence.entity.ProcessDefinitionEntityManager;
import org.activiti.engine.impl.persistence.entity.ProcessDefinitionInfoEntity;
import org.activiti.engine.impl.persistence.entity.ProcessDefinitionInfoEntityManager;
import org.activiti.engine.impl.persistence.entity.ResourceEntity;
import org.activiti.engine.impl.persistence.entity.ResourceEntityManager;
import org.activiti.engine.impl.persistence.entity.SignalEventSubscriptionEntity;
import org.activiti.engine.impl.persistence.entity.TimerEntity;
import org.activiti.engine.impl.util.IoUtil;
import org.activiti.engine.impl.util.TimerUtil;
import org.activiti.engine.runtime.Job;
import org.activiti.engine.task.IdentityLinkType;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * @author Joram Barrez
 * @author Tijs Rademakers
 */
public class BpmnDeployer implements Deployer {

  private static final Logger log = LoggerFactory.getLogger(BpmnDeployer.class);

  public static final String[] BPMN_RESOURCE_SUFFIXES = new String[] { "bpmn20.xml", "bpmn" };
  public static final String[] DIAGRAM_SUFFIXES = new String[] { "png", "jpg", "gif", "svg" };

  protected ExpressionManager expressionManager;
  protected BpmnParser bpmnParser;
  protected IdGenerator idGenerator;

  public void deploy(DeploymentEntity deployment, Map<String, Object> deploymentSettings) {
    log.debug("Processing deployment {}", deployment.getName());
    
    AugmentedDeployment augmented = new AugmentedDeployment.Builder(deployment, deploymentSettings, bpmnParser).build();

    List<ProcessDefinitionEntity> processDefinitions = augmented.getAllProcessDefinitions();
    Map<String, org.activiti.bpmn.model.Process> processModels = augmented.getProcessModelsByKey();
    Map<String, BpmnModel> bpmnModels = augmented.getBpmnModelsByKey();
    Map<String, ResourceEntity> resources = deployment.getResources();

    final ProcessEngineConfigurationImpl processEngineConfiguration = Context.getProcessEngineConfiguration();
    for (String resourceName : resources.keySet()) {

      log.info("Processing resource {}", resourceName);
      if (isBpmnResource(resourceName)) {
        BpmnParse bpmnParse = augmented.bpmnParseForResourceName(resourceName);
        for (ProcessDefinitionEntity processDefinition : augmented.processDefinitionsFromResource(resourceName)) {
          processDefinition.setResourceName(resourceName);

          // Backwards compatibility
          if (deployment.getEngineVersion() != null) {
            processDefinition.setEngineVersion(deployment.getEngineVersion());
          }

          if (deployment.getTenantId() != null) {
            processDefinition.setTenantId(deployment.getTenantId()); // process definition inherits the tenant id
          }

          String diagramResourceName = getDiagramResourceForProcess(resourceName, processDefinition.getKey(), resources);

          // Only generate the resource when deployment is new to prevent modification of deployment resources
          // after the process-definition is actually deployed. Also to prevent resource-generation failure every
          // time the process definition is added to the deployment-cache when diagram-generation has failed the first time.
          if (deployment.isNew()) {
            if (processEngineConfiguration.isCreateDiagramOnDeploy() && diagramResourceName == null && processDefinition.isGraphicalNotationDefined()) {
              try {
                byte[] diagramBytes = IoUtil.readInputStream(
                    processEngineConfiguration.getProcessDiagramGenerator().generateDiagram(bpmnParse.getBpmnModel(), "png", processEngineConfiguration.getActivityFontName(),
                        processEngineConfiguration.getLabelFontName(), processEngineConfiguration.getClassLoader()), null);
                diagramResourceName = getProcessImageResourceName(resourceName, processDefinition.getKey(), "png");
                createResource(processEngineConfiguration.getResourceEntityManager(), diagramResourceName, diagramBytes, deployment);
              } catch (Throwable t) { // if anything goes wrong, we don't store the image (the process will still be executable).
                log.warn("Error while generating process diagram, image will not be stored in repository", t);
              }
            }
          }

          processDefinition.setDiagramResourceName(diagramResourceName);
        }
      }
    }

    // check if there are process definitions with the same process key to
    // prevent database unique index violation
    List<String> keyList = new ArrayList<String>();
    for (ProcessDefinitionEntity processDefinition : processDefinitions) {
      if (keyList.contains(processDefinition.getKey())) {
        throw new ActivitiException("The deployment contains process definitions with the same key (process id attribute), this is not allowed");
      }
      keyList.add(processDefinition.getKey());
    }

    CommandContext commandContext = Context.getCommandContext();
    ProcessDefinitionEntityManager processDefinitionManager = commandContext.getProcessDefinitionEntityManager();
    for (ProcessDefinitionEntity processDefinition : processDefinitions) {
      List<TimerEntity> timers = new ArrayList<TimerEntity>();
      if (deployment.isNew()) {
        int processDefinitionVersion;

        ProcessDefinitionEntity latestProcessDefinition = null;
        if (processDefinition.getTenantId() != null && !ProcessEngineConfiguration.NO_TENANT_ID.equals(processDefinition.getTenantId())) {
          latestProcessDefinition = processDefinitionManager.findLatestProcessDefinitionByKeyAndTenantId(processDefinition.getKey(), processDefinition.getTenantId());
        } else {
          latestProcessDefinition = processDefinitionManager.findLatestProcessDefinitionByKey(processDefinition.getKey());
        }

        if (latestProcessDefinition != null) {
          processDefinitionVersion = latestProcessDefinition.getVersion() + 1;
        } else {
          processDefinitionVersion = 1;
        }

        processDefinition.setVersion(processDefinitionVersion);
        processDefinition.setDeploymentId(deployment.getId());

        String nextId = idGenerator.getNextId();
        String processDefinitionId = processDefinition.getKey() + ":" + processDefinition.getVersion() + ":" + nextId; // ACT-505

        // ACT-115: maximum id length is 64 characters
        if (processDefinitionId.length() > 64) {
          processDefinitionId = nextId;
        }
        processDefinition.setId(processDefinitionId);

        if (commandContext.getProcessEngineConfiguration().getEventDispatcher().isEnabled()) {
          commandContext.getProcessEngineConfiguration().getEventDispatcher().dispatchEvent(ActivitiEventBuilder.createEntityEvent(ActivitiEventType.ENTITY_CREATED, processDefinition));
        }

        org.activiti.bpmn.model.Process process = processModels.get(processDefinition.getKey());

        removeObsoleteTimers(processDefinition);
        addTimerDeclarations(processDefinition, process, timers);

        removeObsoleteMessageEventSubscriptions(processDefinition, latestProcessDefinition);
        addMessageEventSubscriptions(processDefinition, process, bpmnModels.get(processDefinition.getKey()));

        removeObsoleteSignalEventSubScription(processDefinition, latestProcessDefinition);
        addSignalEventSubscriptions(commandContext, processDefinition, process, bpmnModels.get(processDefinition.getKey()));

        commandContext.getProcessDefinitionEntityManager().insert(processDefinition, false);
        addAuthorizationsFromIterator(commandContext, processDefinition.getCandidateStarterUserIdExpressions(), processDefinition, ExprType.USER);
        addAuthorizationsFromIterator(commandContext, processDefinition.getCandidateStarterGroupIdExpressions(), processDefinition, ExprType.GROUP);

        if (commandContext.getProcessEngineConfiguration().getEventDispatcher().isEnabled()) {
          commandContext.getProcessEngineConfiguration().getEventDispatcher().dispatchEvent(ActivitiEventBuilder.createEntityEvent(ActivitiEventType.ENTITY_INITIALIZED, processDefinition));
        }

        scheduleTimers(timers);

      } else {
        String deploymentId = deployment.getId();
        processDefinition.setDeploymentId(deploymentId);

        ProcessDefinitionEntity persistedProcessDefinition = null;
        if (processDefinition.getTenantId() == null || ProcessEngineConfiguration.NO_TENANT_ID.equals(processDefinition.getTenantId())) {
          persistedProcessDefinition = processDefinitionManager.findProcessDefinitionByDeploymentAndKey(deploymentId, processDefinition.getKey());
        } else {
          persistedProcessDefinition = processDefinitionManager.findProcessDefinitionByDeploymentAndKeyAndTenantId(deploymentId, processDefinition.getKey(), processDefinition.getTenantId());
        }

        if (persistedProcessDefinition != null) {
          processDefinition.setId(persistedProcessDefinition.getId());
          processDefinition.setVersion(persistedProcessDefinition.getVersion());
          processDefinition.setSuspensionState(persistedProcessDefinition.getSuspensionState());
        }
      }

      // Add to cache
      ProcessDefinitionCacheEntry cacheEntry = new ProcessDefinitionCacheEntry(processDefinition, bpmnModels.get(processDefinition.getKey()), processModels.get(processDefinition.getKey()));
      processEngineConfiguration.getDeploymentManager().getProcessDefinitionCache().add(processDefinition.getId(), cacheEntry);
      addDefinitionInfoToCache(processDefinition, processEngineConfiguration, commandContext);
      
      // Add to deployment for further usage
      deployment.addDeployedArtifact(processDefinition);
    }
  }
  
  /**
   * @param allProcessDefinitions
   */
  private void verifyNoProcessDefinitionsShareKeys(
      Iterable<ProcessDefinitionEntity> allProcessDefinitions) {
    // TODO(stm): Auto-generated method stub
    
  }

  protected void addDefinitionInfoToCache(ProcessDefinitionEntity processDefinition, 
      ProcessEngineConfigurationImpl processEngineConfiguration, CommandContext commandContext) {
    
    if (processEngineConfiguration.isEnableProcessDefinitionInfoCache() == false) {
      return;
    }
    
    DeploymentManager deploymentManager = processEngineConfiguration.getDeploymentManager();
    ProcessDefinitionInfoEntityManager definitionInfoEntityManager = commandContext.getProcessDefinitionInfoEntityManager();
    ObjectMapper objectMapper = commandContext.getProcessEngineConfiguration().getObjectMapper();
    ProcessDefinitionInfoEntity definitionInfoEntity = definitionInfoEntityManager.findProcessDefinitionInfoByProcessDefinitionId(processDefinition.getId());
    
    ObjectNode infoNode = null;
    if (definitionInfoEntity != null && definitionInfoEntity.getInfoJsonId() != null) {
      byte[] infoBytes = definitionInfoEntityManager.findInfoJsonById(definitionInfoEntity.getInfoJsonId());
      if (infoBytes != null) {
        try {
          infoNode = (ObjectNode) objectMapper.readTree(infoBytes);
        } catch (Exception e) {
          throw new ActivitiException("Error deserializing json info for process definition " + processDefinition.getId());
        }
      }
    }
    
    ProcessDefinitionInfoCacheObject definitionCacheObject = new ProcessDefinitionInfoCacheObject();
    if (definitionInfoEntity == null) {
      definitionCacheObject.setRevision(0);
    } else {
      definitionCacheObject.setId(definitionInfoEntity.getId());
      definitionCacheObject.setRevision(definitionInfoEntity.getRevision());
    }
    
    if (infoNode == null) {
      infoNode = objectMapper.createObjectNode();
    }
    definitionCacheObject.setInfoNode(infoNode);
    
    deploymentManager.getProcessDefinitionInfoCache().add(processDefinition.getId(), definitionCacheObject);
  }

  protected void scheduleTimers(List<TimerEntity> timers) {
    for (TimerEntity timer : timers) {
      Context.getCommandContext().getJobEntityManager().schedule(timer);
    }
  }

  protected void addTimerDeclarations(ProcessDefinitionEntity processDefinition, org.activiti.bpmn.model.Process process, List<TimerEntity> timers) {
    if (CollectionUtils.isNotEmpty(process.getFlowElements())) {
      for (FlowElement element : process.getFlowElements()) {
        if (element instanceof StartEvent) {
          StartEvent startEvent = (StartEvent) element;
          if (CollectionUtils.isNotEmpty(startEvent.getEventDefinitions())) {
            EventDefinition eventDefinition = startEvent.getEventDefinitions().get(0);
            if (eventDefinition instanceof TimerEventDefinition) {
              TimerEventDefinition timerEventDefinition = (TimerEventDefinition) eventDefinition;
              TimerEntity timer = TimerUtil.createTimerEntityForTimerEventDefinition(timerEventDefinition, false, null, TimerStartEventJobHandler.TYPE,
                  TimerEventHandler.createConfiguration(startEvent.getId(), timerEventDefinition.getEndDate()));
              
              if (timer != null) {
                timer.setProcessDefinitionId(processDefinition.getId());
  
                if (processDefinition.getTenantId() != null) {
                  timer.setTenantId(processDefinition.getTenantId());
                }
                timers.add(timer);
              }
              
            }
          }
        }
      }
    }
  }

 protected void removeObsoleteTimers(ProcessDefinitionEntity processDefinition) {
    
    List<Job> jobsToDelete = null;
    
    if (processDefinition.getTenantId() != null && !ProcessEngineConfiguration.NO_TENANT_ID.equals(processDefinition.getTenantId())) {
      jobsToDelete = Context.getCommandContext().getJobEntityManager().findJobsByTypeAndProcessDefinitionKeyAndTenantId(
          TimerStartEventJobHandler.TYPE, processDefinition.getKey(), processDefinition.getTenantId());
    } else {
      jobsToDelete = Context.getCommandContext().getJobEntityManager()
          .findJobsByTypeAndProcessDefinitionKeyNoTenantId(TimerStartEventJobHandler.TYPE, processDefinition.getKey());
    }

    if (jobsToDelete != null) {
      for (Job job :jobsToDelete) {
          new CancelJobsCmd(job.getId()).execute(Context.getCommandContext());
      }
    }
  }

  protected void removeObsoleteMessageEventSubscriptions(ProcessDefinitionEntity processDefinition, ProcessDefinitionEntity latestProcessDefinition) {
    // remove all subscriptions for the previous version
    if (latestProcessDefinition != null) {
      CommandContext commandContext = Context.getCommandContext();

      EventSubscriptionEntityManager eventSubscriptionEntityManager = commandContext.getEventSubscriptionEntityManager();
      List<EventSubscriptionEntity> subscriptionsToDelete = eventSubscriptionEntityManager.findEventSubscriptionsByConfiguration(MessageEventHandler.EVENT_HANDLER_TYPE,
          latestProcessDefinition.getId(), latestProcessDefinition.getTenantId());

      for (EventSubscriptionEntity eventSubscriptionEntity : subscriptionsToDelete) {
        eventSubscriptionEntityManager.delete(eventSubscriptionEntity);
      }

    }
  }

  protected void addMessageEventSubscriptions(ProcessDefinitionEntity processDefinition, Process process, BpmnModel bpmnModel) {
    if (CollectionUtils.isNotEmpty(process.getFlowElements())) {
      for (FlowElement element : process.getFlowElements()) {
        if (element instanceof StartEvent) {
          StartEvent startEvent = (StartEvent) element;
          if (CollectionUtils.isNotEmpty(startEvent.getEventDefinitions())) {
            EventDefinition eventDefinition = startEvent.getEventDefinitions().get(0);
            if (eventDefinition instanceof MessageEventDefinition) {
              MessageEventDefinition messageEventDefinition = (MessageEventDefinition) eventDefinition;
              insertMessageEvent(messageEventDefinition, startEvent, processDefinition, bpmnModel);
            }
          }
        } 
      }
    }
  }
  
  protected void insertMessageEvent(MessageEventDefinition messageEventDefinition, StartEvent startEvent, ProcessDefinitionEntity processDefinition, BpmnModel bpmnModel) {
    
    CommandContext commandContext = Context.getCommandContext();
    if (bpmnModel.containsMessageId(messageEventDefinition.getMessageRef())) {
      Message message = bpmnModel.getMessage(messageEventDefinition.getMessageRef());
      messageEventDefinition.setMessageRef(message.getName());
    }

    // look for subscriptions for the same name in db:
    List<EventSubscriptionEntity> subscriptionsForSameMessageName = commandContext.getEventSubscriptionEntityManager()
        .findEventSubscriptionsByName(MessageEventHandler.EVENT_HANDLER_TYPE, messageEventDefinition.getMessageRef(), processDefinition.getTenantId());

    
    for (EventSubscriptionEntity eventSubscriptionEntity : subscriptionsForSameMessageName) {
      // throw exception only if there's already a subscription as start event
      if (eventSubscriptionEntity.getProcessInstanceId() == null || eventSubscriptionEntity.getProcessInstanceId().isEmpty()) { // processInstanceId != null or not empty -> it's a message related to an execution
        // the event subscription has no instance-id, so it's a message start event
        throw new ActivitiException("Cannot deploy process definition '" + processDefinition.getResourceName()
            + "': there already is a message event subscription for the message with name '" + messageEventDefinition.getMessageRef() + "'.");
      }
    }

    MessageEventSubscriptionEntity newSubscription = commandContext.getEventSubscriptionEntityManager().createMessageEventSubscription();
    newSubscription.setEventName(messageEventDefinition.getMessageRef());
    newSubscription.setActivityId(startEvent.getId());
    newSubscription.setConfiguration(processDefinition.getId());
    newSubscription.setProcessDefinitionId(processDefinition.getId());

    if (processDefinition.getTenantId() != null) {
      newSubscription.setTenantId(processDefinition.getTenantId());
    }

    commandContext.getEventSubscriptionEntityManager().insert(newSubscription);
  }

  protected void removeObsoleteSignalEventSubScription(ProcessDefinitionEntity processDefinition, ProcessDefinitionEntity latestProcessDefinition) {
    // remove all subscriptions for the previous version
    if (latestProcessDefinition != null) {
      CommandContext commandContext = Context.getCommandContext();

      EventSubscriptionEntityManager eventSubscriptionEntityManager = commandContext.getEventSubscriptionEntityManager();
      List<EventSubscriptionEntity> subscriptionsToDelete = eventSubscriptionEntityManager.findEventSubscriptionsByConfiguration(SignalEventHandler.EVENT_HANDLER_TYPE,
          latestProcessDefinition.getId(), latestProcessDefinition.getTenantId());

      for (EventSubscriptionEntity eventSubscriptionEntity : subscriptionsToDelete) {
        eventSubscriptionEntityManager.delete(eventSubscriptionEntity);
      }

    }
  }

  protected void addSignalEventSubscriptions(CommandContext commandContext, ProcessDefinitionEntity processDefinition, org.activiti.bpmn.model.Process process, BpmnModel bpmnModel) {
    if (CollectionUtils.isNotEmpty(process.getFlowElements())) {
      for (FlowElement element : process.getFlowElements()) {
        if (element instanceof StartEvent) {
          StartEvent startEvent = (StartEvent) element;
          if (CollectionUtils.isNotEmpty(startEvent.getEventDefinitions())) {
            EventDefinition eventDefinition = startEvent.getEventDefinitions().get(0);
            if (eventDefinition instanceof SignalEventDefinition) {
              SignalEventDefinition signalEventDefinition = (SignalEventDefinition) eventDefinition;
              SignalEventSubscriptionEntity subscriptionEntity = commandContext.getEventSubscriptionEntityManager().createSignalEventSubscription();
              Signal signal = bpmnModel.getSignal(signalEventDefinition.getSignalRef());
              if (signal != null) {
                subscriptionEntity.setEventName(signal.getName());
              } else {
                subscriptionEntity.setEventName(signalEventDefinition.getSignalRef());
              }
              subscriptionEntity.setActivityId(startEvent.getId());
              subscriptionEntity.setProcessDefinitionId(processDefinition.getId());
              if (processDefinition.getTenantId() != null) {
                subscriptionEntity.setTenantId(processDefinition.getTenantId());
              }
              
              Context.getCommandContext().getEventSubscriptionEntityManager().insert(subscriptionEntity);
            }
          }
        }
      }
    }
  }

  enum ExprType {
    USER, GROUP
  }

  private void addAuthorizationsFromIterator(CommandContext commandContext, Set<Expression> exprSet, ProcessDefinitionEntity processDefinition, ExprType exprType) {
    if (exprSet != null) {
      Iterator<Expression> iterator = exprSet.iterator();
      while (iterator.hasNext()) {
        Expression expr = (Expression) iterator.next();
        IdentityLinkEntity identityLink = commandContext.getIdentityLinkEntityManager().create();
        identityLink.setProcessDef(processDefinition);
        if (exprType.equals(ExprType.USER)) {
          identityLink.setUserId(expr.toString());
        } else if (exprType.equals(ExprType.GROUP)) {
          identityLink.setGroupId(expr.toString());
        }
        identityLink.setType(IdentityLinkType.CANDIDATE);
        Context.getCommandContext().getIdentityLinkEntityManager().insert(identityLink);
      }
    }
  }

  /**
   * Returns the default name of the image resource for a certain process.
   * 
   * It will first look for an image resource which matches the process specifically, before resorting to an image resource which matches the BPMN 2.0 xml file resource.
   * 
   * Example: if the deployment contains a BPMN 2.0 xml resource called 'abc.bpmn20.xml' containing only one process with key 'myProcess', then this method will look for an image resources called
   * 'abc.myProcess.png' (or .jpg, or .gif, etc.) or 'abc.png' if the previous one wasn't found.
   * 
   * Example 2: if the deployment contains a BPMN 2.0 xml resource called 'abc.bpmn20.xml' containing three processes (with keys a, b and c), then this method will first look for an image resource
   * called 'abc.a.png' before looking for 'abc.png' (likewise for b and c). Note that if abc.a.png, abc.b.png and abc.c.png don't exist, all processes will have the same image: abc.png.
   * 
   * @return null if no matching image resource is found.
   */
  protected String getDiagramResourceForProcess(String bpmnFileResource, String processKey, Map<String, ResourceEntity> resources) {
    for (String diagramSuffix : DIAGRAM_SUFFIXES) {
      String diagramForBpmnFileResource = getBpmnFileImageResourceName(bpmnFileResource, diagramSuffix);
      String processDiagramResource = getProcessImageResourceName(bpmnFileResource, processKey, diagramSuffix);
      if (resources.containsKey(processDiagramResource)) {
        return processDiagramResource;
      } else if (resources.containsKey(diagramForBpmnFileResource)) {
        return diagramForBpmnFileResource;
      }
    }
    return null;
  }

  protected String getBpmnFileImageResourceName(String bpmnFileResource, String diagramSuffix) {
    String bpmnFileResourceBase = stripBpmnFileSuffix(bpmnFileResource);
    return bpmnFileResourceBase + diagramSuffix;
  }

  protected String getProcessImageResourceName(String bpmnFileResource, String processKey, String diagramSuffix) {
    String bpmnFileResourceBase = stripBpmnFileSuffix(bpmnFileResource);
    return bpmnFileResourceBase + processKey + "." + diagramSuffix;
  }

  protected String stripBpmnFileSuffix(String bpmnFileResource) {
    for (String suffix : BPMN_RESOURCE_SUFFIXES) {
      if (bpmnFileResource.endsWith(suffix)) {
        return bpmnFileResource.substring(0, bpmnFileResource.length() - suffix.length());
      }
    }
    return bpmnFileResource;
  }

  protected void createResource(ResourceEntityManager resourceEntityManager, String name, byte[] bytes, DeploymentEntity deploymentEntity) {
    ResourceEntity resource = resourceEntityManager.create();
    resource.setName(name);
    resource.setBytes(bytes);
    resource.setDeploymentId(deploymentEntity.getId());

    // Mark the resource as 'generated'
    resource.setGenerated(true);

    resourceEntityManager.insert(resource, false);
  }

  protected boolean isBpmnResource(String resourceName) {
    for (String suffix : BPMN_RESOURCE_SUFFIXES) {
      if (resourceName.endsWith(suffix)) {
        return true;
      }
    }
    return false;
  }

  public ExpressionManager getExpressionManager() {
    return expressionManager;
  }

  public void setExpressionManager(ExpressionManager expressionManager) {
    this.expressionManager = expressionManager;
  }

  public BpmnParser getBpmnParser() {
    return bpmnParser;
  }

  public void setBpmnParser(BpmnParser bpmnParser) {
    this.bpmnParser = bpmnParser;
  }

  public IdGenerator getIdGenerator() {
    return idGenerator;
  }

  public void setIdGenerator(IdGenerator idGenerator) {
    this.idGenerator = idGenerator;
  }

}
