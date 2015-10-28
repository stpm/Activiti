package org.activiti.engine.impl.bpmn.deployer;

import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.Process;
import org.activiti.engine.impl.bpmn.parser.BpmnParse;
import org.activiti.engine.impl.persistence.entity.DeploymentEntity;
import org.activiti.engine.impl.persistence.entity.ProcessDefinitionEntity;
import org.activiti.engine.impl.persistence.entity.ResourceEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
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
  
  public DeploymentEntity getDeployment() {
    return entity;
  }

  public List<ProcessDefinitionEntity> getAllProcessDefinitions() {
    List<ProcessDefinitionEntity> result = new ArrayList<ProcessDefinitionEntity>();
    
    for (AugmentedBpmnParse augmentedParse : parses) {
      result.addAll(augmentedParse.getAllProcessDefinitions());
    }
    
    return result;
  }

  protected BpmnModel getBpmnModelForProcessDefinition(ProcessDefinitionEntity processDefinition) {
    return null;
  }
  
  protected BpmnParse bpmnParseForProcessDefinition(ProcessDefinitionEntity entity) {
    return null;
  }
  
  protected Process getProcessModelForProcessDefinition(ProcessDefinitionEntity processDefinition) {
    return null;
  }
  
  protected Collection<AugmentedBpmnParse> getAugmentedParses() {
    return parses;
  }
  
  public static class Builder {
    private final DeploymentEntity deployment;
    private final AugmentedBpmnParse.Builder parseBuilder;

    public Builder(DeploymentEntity deployment, AugmentedBpmnParse.Builder parseBuilder) {
      this.deployment = deployment;
      this.parseBuilder = parseBuilder;
    }
    
    public AugmentedDeployment build() {
      List<AugmentedBpmnParse> parses = getParses();
      
      return new AugmentedDeployment(deployment, parses);
    }

    protected List<AugmentedBpmnParse> getParses() {
      List<AugmentedBpmnParse> parses = new ArrayList<AugmentedBpmnParse>();
      
      Map<String, ResourceEntity> resources = deployment.getResources();
      for (String resourceName : resources.keySet()) {
        if (isBpmnResource(resourceName)) {
          log.info("Processing BPMN resource {}", resourceName);
          
          parses.add(parseBuilder.buildParseForResource(resources.get(resourceName)));
        }
      }
      
      return parses;
    }
  }
    
  private static boolean isBpmnResource(String resourceName) {
    for (String suffix : BpmnDeployer.BPMN_RESOURCE_SUFFIXES) {
      if (resourceName.endsWith(suffix)) {
        return true;
      }
    }
     
    return false;
  }
}

//  public Map<String, Process> getProcessModelsByKey() {
//    Map<String, Process> result = new LinkedHashMap<String, Process>();
//    for (AugmentedBpmnParse augmentedParse : parses) {
//      BpmnParse unaugmentedParse = augmentedParse.getBpmnParse();
//      
//      for (ProcessDefinitionEntity definition : augmentedParse.getAllProcessDefinitions()) {
//        String key = definition.getKey();
//        Process model = unaugmentedParse.getBpmnModel().getProcessById(key);
//        
//        result.put(key, model);
//      }
//    }   
//    return result;
//  }

//  public Map<String, BpmnModel> getBpmnModelsByKey() {
//    Map<String, BpmnModel> result = new LinkedHashMap<String, BpmnModel>();
//    for (AugmentedBpmnParse augmentedParse : parses) {
//      BpmnParse unaugmentedParse = augmentedParse.getBpmnParse();
//      BpmnModel bpmnModel = unaugmentedParse.getBpmnModel();
//      
//      for (ProcessDefinitionEntity entity : augmentedParse.getAllProcessDefinitions()) {
//        result.put(entity.getKey(), bpmnModel);
//      }
//    }
//    
//    return result;
//  }
  

//  protected AugmentedBpmnParse getAugmentedParseByResourceName(String resourceName) {
//    // TODO(stm): make this much more efficient.
//    for (AugmentedBpmnParse augmentedParse : parses) {
//      if (augmentedParse.getResourceName().equals(resourceName)) {
//        return augmentedParse;
//      }
//    }
//    return null;
//  }
  
//  public Collection<ProcessDefinitionEntity> processDefinitionsFromResource(String resourceName) {
//    return getAugmentedParseByResourceName(resourceName).getAllProcessDefinitions();
//  }

//  public BpmnParse bpmnParseForResourceName(String resourceName) {
//    return getAugmentedParseByResourceName(resourceName).getBpmnParse();
//  }

