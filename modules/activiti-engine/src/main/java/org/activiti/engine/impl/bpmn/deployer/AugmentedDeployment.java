package org.activiti.engine.impl.bpmn.deployer;

import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.Process;
import org.activiti.engine.impl.bpmn.parser.BpmnParse;
import org.activiti.engine.impl.bpmn.parser.BpmnParser;
import org.activiti.engine.impl.cmd.DeploymentSettings;
import org.activiti.engine.impl.persistence.entity.DeploymentEntity;
import org.activiti.engine.impl.persistence.entity.DeploymentEntityImpl;
import org.activiti.engine.impl.persistence.entity.ProcessDefinitionEntity;
import org.activiti.engine.impl.persistence.entity.ResourceEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A DeploymentEntity along with all the associated process definitions and BPMN models. 
 */
public class AugmentedDeployment {
  private static final Logger log = LoggerFactory.getLogger(BpmnDeployer.class);

  private DeploymentEntity entity;
  private Collection<AugmentedBpmnParse> parses;

  private AugmentedDeployment(DeploymentEntity entity,
      Collection<AugmentedBpmnParse> parses) {
    this.entity = entity;
    this.parses = parses;
  }
  
  public static class Builder {
    private final DeploymentEntity deployment;
    private final BpmnParser bpmnParser;
    private final Map<String, Object> deploymentSettings;

    public Builder(DeploymentEntity deployment,
        Map<String, Object> settings,
        BpmnParser parser) {
      this.deployment = deployment;
      this.deploymentSettings = settings;
      this.bpmnParser = parser;
    }
    
    public AugmentedDeployment build() {
      List<AugmentedBpmnParse> parses = new ArrayList<AugmentedBpmnParse>();
      
      Map<String, ResourceEntity> resources = deployment.getResources();
      for (String resourceName : resources.keySet()) {
        if (isBpmnResource(resourceName)) {
          log.info("Processing BPMN resource {}", resourceName);

          ByteArrayInputStream inputStream = new ByteArrayInputStream(resources.get(resourceName).getBytes());
          BpmnParse executedParse = createBpmnParse(resourceName, inputStream);
          
          parses.add(new AugmentedBpmnParse(executedParse, resourceName));
        }
      }

      return new AugmentedDeployment(deployment, parses);
    }

    protected BpmnParse createBpmnParse(String resourceName, ByteArrayInputStream inputStream) {
      BpmnParse bpmnParse = bpmnParser.createParse()
          .sourceInputStream(inputStream)
          .setSourceSystemId(resourceName)
          .deployment(deployment)
          .name(resourceName);

      if (deploymentSettings != null) {

        // Schema validation if needed
        if (deploymentSettings.containsKey(DeploymentSettings.IS_BPMN20_XSD_VALIDATION_ENABLED)) {
          bpmnParse.setValidateSchema((Boolean) deploymentSettings.get(DeploymentSettings.IS_BPMN20_XSD_VALIDATION_ENABLED));
        }

        // Process validation if needed
        if (deploymentSettings.containsKey(DeploymentSettings.IS_PROCESS_VALIDATION_ENABLED)) {
          bpmnParse.setValidateProcess((Boolean) deploymentSettings.get(DeploymentSettings.IS_PROCESS_VALIDATION_ENABLED));
        }

      } else {
        // On redeploy, we assume it is validated at the first
        // deploy
        bpmnParse.setValidateSchema(false);
        bpmnParse.setValidateProcess(false);
      }
      bpmnParse.execute();
      return bpmnParse;
    }
  }
    
  public static boolean isBpmnResource(String resourceName) {
    for (String suffix : BpmnDeployer.BPMN_RESOURCE_SUFFIXES) {
      if (resourceName.endsWith(suffix)) {
        return true;
      }
    }
     
    return false;
  }
  
  private static class AugmentedBpmnParse {
    private final BpmnParse alreadyExecutedParse;
    private final String resourceName;

    private final Collection<ProcessDefinitionEntity> definitions;
    
    public AugmentedBpmnParse(BpmnParse alreadyExecutedParse, String resourceName) {
      this.alreadyExecutedParse = alreadyExecutedParse;
      this.resourceName = resourceName;

      this.definitions = 
          Collections.unmodifiableList(alreadyExecutedParse.getProcessDefinitions());
    }
    
    public Collection<ProcessDefinitionEntity> getAllProcessDefinitions() {
      return definitions;
    }
    
    public BpmnParse getBpmnParse() {
      return alreadyExecutedParse;
    }
    
    public String getResourceName() {
      return resourceName;
    }
  }

  public List<ProcessDefinitionEntity> getAllProcessDefinitions() {
    List<ProcessDefinitionEntity> result = new ArrayList<ProcessDefinitionEntity>();
    
    for (AugmentedBpmnParse augmentedParse : parses) {
      result.addAll(augmentedParse.getAllProcessDefinitions());
    }
    
    return result;
  }

  public Map<String, Process> getProcessModelsByKey() {
    Map<String, Process> result = new LinkedHashMap<String, Process>();
    for (AugmentedBpmnParse augmentedParse : parses) {
      BpmnParse unaugmentedParse = augmentedParse.getBpmnParse();
      
      for (ProcessDefinitionEntity definition : augmentedParse.getAllProcessDefinitions()) {
        String key = definition.getKey();
        Process model = unaugmentedParse.getBpmnModel().getProcessById(key);
        
        result.put(key, model);
      }
    }   
    return result;
  }

  public Map<String, BpmnModel> getBpmnModelsByKey() {
    Map<String, BpmnModel> result = new LinkedHashMap<String, BpmnModel>();
    for (AugmentedBpmnParse augmentedParse : parses) {
      BpmnParse unaugmentedParse = augmentedParse.getBpmnParse();
      BpmnModel bpmnModel = unaugmentedParse.getBpmnModel();
      
      for (ProcessDefinitionEntity entity : augmentedParse.getAllProcessDefinitions()) {
        result.put(entity.getKey(), bpmnModel);
      }
    }
    
    return result;
  }

  protected AugmentedBpmnParse getAugmentedParseByResourceName(String resourceName) {
    // TODO(stm): make this much more efficient.
    for (AugmentedBpmnParse augmentedParse : parses) {
      if (augmentedParse.getResourceName().equals(resourceName)) {
        return augmentedParse;
      }
    }
    return null;
  }
  
  public Collection<ProcessDefinitionEntity> processDefinitionsFromResource(String resourceName) {
    return getAugmentedParseByResourceName(resourceName).getAllProcessDefinitions();
  }

  public BpmnParse bpmnParseForResourceName(String resourceName) {
    return getAugmentedParseByResourceName(resourceName).getBpmnParse();
  }
}

