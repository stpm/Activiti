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
public class ExpandedDeployment {
  private static final Logger log = LoggerFactory.getLogger(BpmnDeployer.class);

  private DeploymentEntity deploymentEntity;
  private Collection<ExpandedBpmnParse> parses;

  private ExpandedDeployment(DeploymentEntity entity,
      Collection<ExpandedBpmnParse> parses) {
    this.deploymentEntity = entity;
    this.parses = parses;
  }
  
  public DeploymentEntity getDeployment() {
    return deploymentEntity;
  }

  public List<ProcessDefinitionEntity> getAllProcessDefinitions() {
    List<ProcessDefinitionEntity> result = new ArrayList<ProcessDefinitionEntity>();
    
    for (ExpandedBpmnParse augmentedParse : parses) {
      result.addAll(augmentedParse.getAllProcessDefinitions());
    }
    
    return result;
  }

  public ExpandedBpmnParse getAugmentedParseForProcessDefinition(ProcessDefinitionEntity entity) {
    for (ExpandedBpmnParse augmentedParse : getAugmentedParses()) {
      if (augmentedParse.getAllProcessDefinitions().contains(entity)) {
        return augmentedParse;
      }
    }
    
    return null;
  }

  public BpmnParse getBpmnParseForProcessDefinition(ProcessDefinitionEntity processDefinition) {
    ExpandedBpmnParse augmented = getAugmentedParseForProcessDefinition(processDefinition);
    
    return (augmented == null ? null : augmented.getBpmnParse());
  }

  public BpmnModel getBpmnModelForProcessDefinition(ProcessDefinitionEntity processDefinition) {
    BpmnParse parse = getBpmnParseForProcessDefinition(processDefinition);
    
    return (parse == null ? null : parse.getBpmnModel());
  }
  
  public Process getProcessModelForProcessDefinition(ProcessDefinitionEntity processDefinition) {
    BpmnModel model = getBpmnModelForProcessDefinition(processDefinition);

    return (model == null ? null : model.getProcessById(processDefinition.getKey()));
  }
  
  protected Collection<ExpandedBpmnParse> getAugmentedParses() {
    return parses;
  }
  
  public static class Builder {
    private final DeploymentEntity deployment;
    private final ExpandedBpmnParse.Builder parseBuilder;

    public Builder(DeploymentEntity deployment, ExpandedBpmnParse.Builder parseBuilder) {
      this.deployment = deployment;
      this.parseBuilder = parseBuilder;
    }
    
    public ExpandedDeployment build() {
      List<ExpandedBpmnParse> parses = getParses();
      
      return new ExpandedDeployment(deployment, parses);
    }

    protected List<ExpandedBpmnParse> getParses() {
      List<ExpandedBpmnParse> parses = new ArrayList<ExpandedBpmnParse>();
      
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

