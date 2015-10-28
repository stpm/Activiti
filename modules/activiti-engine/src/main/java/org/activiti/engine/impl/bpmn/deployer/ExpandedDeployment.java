package org.activiti.engine.impl.bpmn.deployer;

import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.Process;
import org.activiti.engine.impl.bpmn.parser.BpmnParse;
import org.activiti.engine.impl.bpmn.parser.BpmnParser;
import org.activiti.engine.impl.cmd.DeploymentSettings;
import org.activiti.engine.impl.persistence.entity.DeploymentEntity;
import org.activiti.engine.impl.persistence.entity.ProcessDefinitionEntity;
import org.activiti.engine.impl.persistence.entity.ResourceEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
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
    
    for (ExpandedBpmnParse expandedParse : parses) {
      result.addAll(expandedParse.getAllProcessDefinitions());
    }
    
    return result;
  }

  private ExpandedBpmnParse getExpandedParseForProcessDefinition(ProcessDefinitionEntity entity) {
    for (ExpandedBpmnParse expandedParse : getExpandedParses()) {
      if (expandedParse.getAllProcessDefinitions().contains(entity)) {
        return expandedParse;
      }
    }
    
    return null;
  }
  
  public ResourceEntity getResourceForProcessDefinition(ProcessDefinitionEntity processDefinition) {
    ExpandedBpmnParse expandedParse = getExpandedParseForProcessDefinition(processDefinition);
    
    return (expandedParse == null ? null : expandedParse.getResource());
  }

  public BpmnParse getBpmnParseForProcessDefinition(ProcessDefinitionEntity processDefinition) {
    ExpandedBpmnParse expanded = getExpandedParseForProcessDefinition(processDefinition);
    
    return (expanded == null ? null : expanded.getBpmnParse());
  }

  public BpmnModel getBpmnModelForProcessDefinition(ProcessDefinitionEntity processDefinition) {
    BpmnParse parse = getBpmnParseForProcessDefinition(processDefinition);
    
    return (parse == null ? null : parse.getBpmnModel());
  }
  
  public Process getProcessModelForProcessDefinition(ProcessDefinitionEntity processDefinition) {
    BpmnModel model = getBpmnModelForProcessDefinition(processDefinition);

    return (model == null ? null : model.getProcessById(processDefinition.getKey()));
  }
  
  protected Collection<ExpandedBpmnParse> getExpandedParses() {
    return parses;
  }
  
  public static class Builder {
    private final DeploymentEntity deployment;
    private final BpmnParser bpmnParser;
    private final Map<String, Object> deploymentSettings;

    public Builder(DeploymentEntity deployment, BpmnParser bpmnParser, Map<String, Object> deploymentSettings) {
      this.deployment = deployment;
      this.bpmnParser = bpmnParser;
      this.deploymentSettings = deploymentSettings;
    }

    public ExpandedDeployment build() {
      List<ExpandedBpmnParse> parses = getParses();

      return new ExpandedDeployment(deployment, parses);
    }
    
    private List<ExpandedBpmnParse> getParses() {
      List<ExpandedBpmnParse> result = new ArrayList<ExpandedBpmnParse>();
      
      for (ResourceEntity resource : deployment.getResources().values()) {
        if (isBpmnResource(resource.getName())) {
          log.debug("Processing BPMN resource {}", resource.getName());
          ExpandedBpmnParse parse = buildParseForResource(resource);
          result.add(parse);
        }
      }
      
      return result;
    }

    private ExpandedBpmnParse buildParseForResource(ResourceEntity resource) {
      ByteArrayInputStream inputStream = new ByteArrayInputStream(resource.getBytes());
      BpmnParse executedParse = createBpmnParse(resource.getName(), inputStream);

      return new ExpandedBpmnParse(executedParse, resource);
    }

    private BpmnParse createBpmnParse(String resourceName, ByteArrayInputStream inputStream) {
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
  
  public static class BuilderFactory {
    protected BpmnParser bpmnParser;
    
    public BpmnParser getBpmnParser() {
      return bpmnParser;
    }
    
    public void setBpmnParser(BpmnParser bpmnParser) {
      this.bpmnParser = bpmnParser;
    }
    
    public Builder getBuilderForDeployment(DeploymentEntity deployment) {
      return getBuilderForDeploymentAndSettings(deployment, null);
    }
    
    public Builder getBuilderForDeploymentAndSettings(DeploymentEntity deployment, Map<String, Object> deploymentSettings) {
      return new Builder(deployment, bpmnParser, deploymentSettings);
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

