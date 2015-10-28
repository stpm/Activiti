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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
import org.activiti.engine.impl.context.Context;
import org.activiti.engine.impl.el.ExpressionManager;
import org.activiti.engine.impl.event.MessageEventHandler;
import org.activiti.engine.impl.event.SignalEventHandler;
import org.activiti.engine.impl.interceptor.CommandContext;
import org.activiti.engine.impl.jobexecutor.TimerEventHandler;
import org.activiti.engine.impl.jobexecutor.TimerStartEventJobHandler;
import org.activiti.engine.impl.persistence.deploy.Deployer;
import org.activiti.engine.impl.persistence.deploy.DeploymentCache;
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

  @Override
  public void deploy(DeploymentEntity deployment, Map<String, Object> deploymentSettings) {
    log.debug("Processing deployment {}", deployment.getName());

    AugmentedBpmnParse.Builder parseBuilder = new AugmentedBpmnParse.Builder(deployment, bpmnParser, deploymentSettings);
    AugmentedDeployment augmentedDeployment = new AugmentedDeployment.Builder(deployment, parseBuilder).build();
    
    verifyNoProcessDefinitionsShareKeys(augmentedDeployment.getAllProcessDefinitions());

    makeProcessDefinitionsTakeValuesFromDeployment(augmentedDeployment);
    setResourceNamesOnProcessDefinitions(augmentedDeployment);
    
    createAndPersistNewDiagramsAsNeeded(augmentedDeployment);
    setProcessDefinitionDiagramNames(augmentedDeployment);
    
    if (deployment.isNew()) {
      Map<ProcessDefinitionEntity, ProcessDefinitionEntity> mapOfNewProcessDefinitionToPreviousVersion =
          getPreviousVersionsOfProcessDefinitions(augmentedDeployment);
      setProcessDefinitionVersionsAndIds(mapOfNewProcessDefinitionToPreviousVersion, augmentedDeployment);
      persistProcessDefinitions(augmentedDeployment);

      updateTimersAndEvents(augmentedDeployment, mapOfNewProcessDefinitionToPreviousVersion);
    } else {
      makeProcessDefinitionsConsistentWithPersistedVersions(augmentedDeployment);
    }
    
    updateCachingAndArtifacts(augmentedDeployment);
  }

  /**
   * Updates all the process definition entities to match the deployment's values for tenant,
   * engine version, and deployment id.
   */
  protected void makeProcessDefinitionsTakeValuesFromDeployment(AugmentedDeployment augmentedDeployment) {
    DeploymentEntity deployment = augmentedDeployment.getDeployment();
    String engineVersion = deployment.getEngineVersion();
    String tenantId = deployment.getTenantId();
    String deploymentId = deployment.getId();
    
    for (ProcessDefinitionEntity processDefinition : augmentedDeployment.getAllProcessDefinitions()) {
      // Backwards compatibility
      if (engineVersion != null) {
        processDefinition.setEngineVersion(engineVersion);
      }

      if (tenantId != null) {
        processDefinition.setTenantId(tenantId); // process definition inherits the tenant id
      }
      
      processDefinition.setDeploymentId(deploymentId);
    }
  }

  /**
   * Updates all the process definition entities to have the correct resource names.
   */
  protected void setResourceNamesOnProcessDefinitions(AugmentedDeployment augmentedDeployment) {
    for (ProcessDefinitionEntity processDefinition : augmentedDeployment.getAllProcessDefinitions()) {
      String resourceName = augmentedDeployment.getAugmentedParseForProcessDefinition(processDefinition).getResourceName();
      processDefinition.setResourceName(resourceName);
      
    }
  }

  /**
   * Creates new diagrams for process definitions if the deployment is new, the process definition in
   * question supports it, and the engine is configured to make new diagrams.  When this method
   * creates a new diagram, it also persists it via the ResourceEntityManager and adds it to the
   * resources of the deployment.
   */
  protected void createAndPersistNewDiagramsAsNeeded(AugmentedDeployment augmentedDeployment) {
    final ProcessEngineConfigurationImpl processEngineConfiguration = Context.getProcessEngineConfiguration();
    DeploymentEntity deployment = augmentedDeployment.getDeployment();
    
    if (augmentedDeployment.getDeployment().isNew() && processEngineConfiguration.isCreateDiagramOnDeploy()) {
      ResourceEntityManager resourceEntityManager = processEngineConfiguration.getResourceEntityManager();
      
      for (ProcessDefinitionEntity processDefinition : augmentedDeployment.getAllProcessDefinitions()) {
        if (processDefinition.isGraphicalNotationDefined()) {
          String resourceName = processDefinition.getResourceName();
          String diagramResourceName = getDiagramResourceForProcess(
              resourceName, // was already set up in setResourceNamesOnProcessDefinitions called from deploy 
              processDefinition.getKey(), 
              augmentedDeployment.getDeployment().getResources());
          if (diagramResourceName == null) { // didn't find anything
            BpmnParse bpmnParse = augmentedDeployment.getBpmnParseForProcessDefinition(processDefinition);
            try {
              byte[] diagramBytes = IoUtil.readInputStream(
                  processEngineConfiguration.getProcessDiagramGenerator().generateDiagram(bpmnParse.getBpmnModel(), "png", processEngineConfiguration.getActivityFontName(),
                      processEngineConfiguration.getLabelFontName(), processEngineConfiguration.getClassLoader()), null);
              diagramResourceName = getProcessImageResourceName(resourceName, processDefinition.getKey(), "png");
              
              ResourceEntity resource = resourceEntityManager.create();
              resource.setName(diagramResourceName);
              resource.setBytes(diagramBytes);
              resource.setDeploymentId(deployment.getId());

              // Mark the resource as 'generated'
              resource.setGenerated(true);

              resourceEntityManager.insert(resource, false);
              
              deployment.addResource(resource);  // now we'll find it when we look for the diagram name.
            } catch (Throwable t) { // if anything goes wrong, we don't store the image (the process will still be executable).
              log.warn("Error while generating process diagram, image will not be stored in repository", t);
            }
          }
        }
      }
    }
  }
  
  /**
   * Updates all the process definition entities to have the correct diagram resource name.  Must
   * be called after createAndPersistNewDiagramsAsNeeded to ensure that any newly-created diagrams
   * already have their resources attached to the deployment.
   */
  protected void setProcessDefinitionDiagramNames(AugmentedDeployment augmentedDeployment) {
    Map<String, ResourceEntity> resources = augmentedDeployment.getDeployment().getResources();

    for (ProcessDefinitionEntity processDefinition : augmentedDeployment.getAllProcessDefinitions()) {
      String diagramResourceName = getDiagramResourceForProcess(
          processDefinition.getResourceName(), processDefinition.getKey(), resources);
      processDefinition.setDiagramResourceName(diagramResourceName);
    }
  }

  /**
   * Verifies that no two process definitions share the same key, to prevent database unique
   * index violation.
   * 
   * @throws ActivitiException if two or more processes have the same key
   */
  protected void verifyNoProcessDefinitionsShareKeys(Iterable<ProcessDefinitionEntity> processDefinitions) {
    Set<String> keySet = new LinkedHashSet<String>();
    for (ProcessDefinitionEntity processDefinition : processDefinitions) {
      if (keySet.contains(processDefinition.getKey())) {
        throw new ActivitiException("The deployment contains process definitions with the same key (process id attribute), this is not allowed");
      }
      keySet.add(processDefinition.getKey());
    }
  }

  protected Map<ProcessDefinitionEntity, ProcessDefinitionEntity> getPreviousVersionsOfProcessDefinitions(AugmentedDeployment augmentedDeployment) {
    Map<ProcessDefinitionEntity, ProcessDefinitionEntity> result = new LinkedHashMap<ProcessDefinitionEntity, ProcessDefinitionEntity>();
    CommandContext commandContext = Context.getCommandContext();
    ProcessDefinitionEntityManager processDefinitionManager = commandContext.getProcessDefinitionEntityManager();
    String tenantId = augmentedDeployment.getDeployment().getTenantId();
    
    for (ProcessDefinitionEntity newDefinition : augmentedDeployment.getAllProcessDefinitions()) {
      String key = newDefinition.getKey();

      ProcessDefinitionEntity existingDefinition = null;
      
      if (tenantId != null && !tenantId.equals(ProcessEngineConfiguration.NO_TENANT_ID)) {
        existingDefinition = processDefinitionManager.findLatestProcessDefinitionByKeyAndTenantId(key, tenantId);
      } else {
        existingDefinition = processDefinitionManager.findLatestProcessDefinitionByKey(key);
      }
      
      if (existingDefinition != null) {
        result.put(newDefinition, existingDefinition);
      }
    }
    
    return result;
  }
  
  /**
   * For each process definition, determines what the version should be by finding the latest
   * existing process definition with the same key and (if set) tenant, and incrementing that.
   * Modifies the process definitions in place.  If there isn't a version of the process
   * definition already present, then this sets the version to 1.
   */
  protected void setProcessDefinitionVersionsAndIds(Map<ProcessDefinitionEntity, ProcessDefinitionEntity> mapNewToOldProcessDefinitions,
      AugmentedDeployment augmentedDeployment) {
    CommandContext commandContext = Context.getCommandContext();

    for (ProcessDefinitionEntity processDefinition : augmentedDeployment.getAllProcessDefinitions()) {
      int version = 1;
      
      ProcessDefinitionEntity latest = mapNewToOldProcessDefinitions.get(processDefinition);
      if (latest != null) {
        version = latest.getVersion() + 1;
      }
      
      processDefinition.setVersion(version);
      processDefinition.setId(getIdForNewProcessDefinition(processDefinition));
      
      if (commandContext.getProcessEngineConfiguration().getEventDispatcher().isEnabled()) {
        commandContext.getProcessEngineConfiguration().getEventDispatcher().dispatchEvent(ActivitiEventBuilder.createEntityEvent(ActivitiEventType.ENTITY_CREATED, processDefinition));
      }
    }
  }
  
  /**
   * Saves each process definition.  It is assumed that the deployment is new, the definitions
   * have never been saved before, and that they have all their values properly set up.
   */
  protected void persistProcessDefinitions(AugmentedDeployment augmentedDeployment) {
    CommandContext commandContext = Context.getCommandContext();
    ProcessDefinitionEntityManager processDefinitionManager = commandContext.getProcessDefinitionEntityManager();
    
    for (ProcessDefinitionEntity processDefinition : augmentedDeployment.getAllProcessDefinitions()) {
      processDefinitionManager.insert(processDefinition, false);
      
      addAuthorizationsFromIterator(commandContext, processDefinition.getCandidateStarterUserIdExpressions(), processDefinition, ExprType.USER);
      addAuthorizationsFromIterator(commandContext, processDefinition.getCandidateStarterGroupIdExpressions(), processDefinition, ExprType.GROUP);

      if (commandContext.getProcessEngineConfiguration().getEventDispatcher().isEnabled()) {
        commandContext.getProcessEngineConfiguration().getEventDispatcher().dispatchEvent(ActivitiEventBuilder.createEntityEvent(ActivitiEventType.ENTITY_INITIALIZED, processDefinition));
      }
    }
  }
  
  protected void updateTimersAndEvents(AugmentedDeployment augmentedDeployment, Map<ProcessDefinitionEntity, ProcessDefinitionEntity> mapNewToOldProcessDefinitions) {
    CommandContext commandContext = Context.getCommandContext();
    for (ProcessDefinitionEntity processDefinition : augmentedDeployment.getAllProcessDefinitions()) {
      Process process = augmentedDeployment.getProcessModelForProcessDefinition(processDefinition);
      BpmnModel bpmnModel = augmentedDeployment.getBpmnModelForProcessDefinition(processDefinition);
      
      ProcessDefinitionEntity previousProcessDefinition = mapNewToOldProcessDefinitions.get(processDefinition);
      
      removeObsoleteMessageEventSubscriptions(previousProcessDefinition);
      addMessageEventSubscriptions(processDefinition, process, bpmnModel);

      removeObsoleteSignalEventSubScription(previousProcessDefinition);
      addSignalEventSubscriptions(commandContext, processDefinition, process, bpmnModel);
      
      removeObsoleteTimers(processDefinition);
      scheduleTimers(getTimerDeclarations(processDefinition, process));
    }
  }
  
  /**
   * Returns the ID to use for a new process definition; subclasses may override this to provide
   * their own identification scheme.
   */
  protected String getIdForNewProcessDefinition(ProcessDefinitionEntity processDefinition) {
    String nextId = idGenerator.getNextId();
    return processDefinition.getKey() + ":" + processDefinition.getVersion() + ":" + nextId; // ACT-505
  }
  
  /**
   * Loads the persisted version of each process definition and set values on the in-memory
   * version to be consistent.
   */
  protected void makeProcessDefinitionsConsistentWithPersistedVersions(AugmentedDeployment augmentedDeployment) {
    String deploymentId = augmentedDeployment.getDeployment().getId();
    CommandContext commandContext = Context.getCommandContext();
    ProcessDefinitionEntityManager processDefinitionManager = commandContext.getProcessDefinitionEntityManager();

    for (ProcessDefinitionEntity processDefinition : augmentedDeployment.getAllProcessDefinitions()) {
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
  }
  
  /**
   * Ensures that the process definition is cached in the appropriate places, including the
   * deployment's collection of deployed artifacts and the deployment manager's cache, as well
   * as caching any ProcessDefinitionInfos.
   */
  protected void updateCachingAndArtifacts(AugmentedDeployment augmentedDeployment) {
    CommandContext commandContext = Context.getCommandContext();
    final ProcessEngineConfigurationImpl processEngineConfiguration = Context.getProcessEngineConfiguration();
    DeploymentCache<ProcessDefinitionCacheEntry> processDefinitionCache = processEngineConfiguration.getDeploymentManager().getProcessDefinitionCache();
    DeploymentEntity deployment = augmentedDeployment.getDeployment();

    for (ProcessDefinitionEntity processDefinition : augmentedDeployment.getAllProcessDefinitions()) {
      BpmnModel bpmnModel = augmentedDeployment.getBpmnModelForProcessDefinition(processDefinition);
      Process process = augmentedDeployment.getProcessModelForProcessDefinition(processDefinition);
      ProcessDefinitionCacheEntry cacheEntry = new ProcessDefinitionCacheEntry(processDefinition, bpmnModel, process);
      processDefinitionCache.add(processDefinition.getId(), cacheEntry);
      addDefinitionInfoToCache(processDefinition, processEngineConfiguration, commandContext);
    
      // Add to deployment for further usage
      deployment.addDeployedArtifact(processDefinition);
    }
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

  protected List<TimerEntity> getTimerDeclarations(ProcessDefinitionEntity processDefinition, org.activiti.bpmn.model.Process process) {
    List<TimerEntity> timers = new ArrayList<TimerEntity>();
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
    
    return timers;
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

  protected void removeObsoleteMessageEventSubscriptions(ProcessDefinitionEntity previousProcessDefinition) {
    // remove all subscriptions for the previous version
    if (previousProcessDefinition != null) {
      CommandContext commandContext = Context.getCommandContext();

      EventSubscriptionEntityManager eventSubscriptionEntityManager = commandContext.getEventSubscriptionEntityManager();
      List<EventSubscriptionEntity> subscriptionsToDelete = eventSubscriptionEntityManager.findEventSubscriptionsByConfiguration(MessageEventHandler.EVENT_HANDLER_TYPE,
          previousProcessDefinition.getId(), previousProcessDefinition.getTenantId());

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

  protected void removeObsoleteSignalEventSubScription(ProcessDefinitionEntity previousProcessDefinition) {
    // remove all subscriptions for the previous version
    if (previousProcessDefinition != null) {
      CommandContext commandContext = Context.getCommandContext();

      EventSubscriptionEntityManager eventSubscriptionEntityManager = commandContext.getEventSubscriptionEntityManager();
      List<EventSubscriptionEntity> subscriptionsToDelete = eventSubscriptionEntityManager.findEventSubscriptionsByConfiguration(SignalEventHandler.EVENT_HANDLER_TYPE,
          previousProcessDefinition.getId(), previousProcessDefinition.getTenantId());

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
        @SuppressWarnings("cast")
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
