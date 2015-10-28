package org.activiti.engine.impl.bpmn.deployer;

import org.activiti.engine.impl.bpmn.parser.BpmnParse;
import org.activiti.engine.impl.bpmn.parser.BpmnParser;
import org.activiti.engine.impl.cmd.DeploymentSettings;
import org.activiti.engine.impl.persistence.entity.DeploymentEntity;
import org.activiti.engine.impl.persistence.entity.ProcessDefinitionEntity;
import org.activiti.engine.impl.persistence.entity.ResourceEntity;

import java.io.ByteArrayInputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

class AugmentedBpmnParse {
  /**
   * The BpmnParse set up for a given resource, already executed.
   */
  private final BpmnParse alreadyExecutedParse;
  /**
   * The resource from which this was created.
   */
  private final String resourceName;

  /**
   * All the process definitions associated with this parse.
   */
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
  
  protected static class Builder {
    private final DeploymentEntity deployment;
    private final BpmnParser bpmnParser;
    private final Map<String, Object> deploymentSettings;
    
    public Builder(DeploymentEntity deployment, BpmnParser bpmnParser, Map<String, Object> deploymentSettings) {
      this.deployment = deployment;
      this.bpmnParser = bpmnParser;
      this.deploymentSettings = deploymentSettings;
    }
    
    public AugmentedBpmnParse buildParseForResource(ResourceEntity resource) {
      ByteArrayInputStream inputStream = new ByteArrayInputStream(resource.getBytes());
      BpmnParse executedParse = createBpmnParse(resource.getName(), inputStream);
      
      return new AugmentedBpmnParse(executedParse, resource.getName());
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
}