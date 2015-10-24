package org.activiti.engine.impl.bpmn.deployer;

import org.activiti.bpmn.model.BpmnModel;
import org.activiti.engine.impl.bpmn.parser.BpmnParse;
import org.activiti.engine.impl.bpmn.parser.BpmnParser;
import org.activiti.engine.impl.cmd.DeploymentSettings;
import org.activiti.engine.impl.persistence.entity.DeploymentEntity;
import org.activiti.engine.impl.persistence.entity.ProcessDefinitionEntity;
import org.activiti.engine.impl.persistence.entity.ResourceEntity;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A wrapper around a a BPMN model and the associated process definitions.
 */
public class BpmnModelAndProcessDefinitions {
  private final BpmnModel model;
  private final Iterable<ProcessDefinitionEntity> processDefinitions;
  
  public BpmnModelAndProcessDefinitions(BpmnModel model, Iterable<ProcessDefinitionEntity> processDefinitions) {
    this.model = model;
    this.processDefinitions = processDefinitions;
  }
  
  public Iterable<ProcessDefinitionEntity> getProcessDefinitions() {
    return processDefinitions;
  }
  
  public BpmnModel getModel() {
    return model;
  }
  
  public static Iterable<ProcessDefinitionEntity> getProcessDefinitionEntities(
      Iterable<BpmnModelAndProcessDefinitions> wrappers) {
    List<ProcessDefinitionEntity> result = new ArrayList<ProcessDefinitionEntity>();
    
    for (BpmnModelAndProcessDefinitions modelAndDefinitions : wrappers) {
      for (ProcessDefinitionEntity definition : modelAndDefinitions.getProcessDefinitions()) {
        result.add(definition);
      }
    }

    return result;
  }
  
  public static class IterableBuilder {
    private DeploymentEntity deployment;
    private BpmnParser bpmnParser;
    private Map<String, Object> deploymentSettings;
    
    private IterableBuilder(DeploymentEntity entity) {
      this.deployment = entity;
    }
    
    public static IterableBuilder forDeployment(DeploymentEntity entity) {
      return new IterableBuilder(entity);
    }
    
    public IterableBuilder parser(BpmnParser parser) {
      this.bpmnParser = parser;
      
      return this;
    }
    
    public IterableBuilder settings(Map<String, Object> settings) {
      this.deploymentSettings = settings;
      
      return this;
    }
    
    public Iterable<BpmnModelAndProcessDefinitions> build() {
      List<BpmnModelAndProcessDefinitions> result = new ArrayList<BpmnModelAndProcessDefinitions>();
      
      for (String resourceName : deployment.getResources().keySet()) {
        if (isBpmnResource(resourceName)) {
          result.add(createFromResource(deployment.getResources().get(resourceName)));
        }
      }
      
      return result;
    }
    
    protected BpmnModelAndProcessDefinitions createFromResource(ResourceEntity resource) {
      String resourceName = resource.getName();
      byte[] bytes = resource.getBytes();
      ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);

      BpmnParse bpmnParse = createBpmnParse(resourceName, inputStream);
      bpmnParse.execute();

      List<ProcessDefinitionEntity> processDefinitions = bpmnParse.getProcessDefinitions();
      for (ProcessDefinitionEntity oneDefinition : processDefinitions) {
        oneDefinition.setResourceName(resourceName);
        
        // Backwards compatibility
        if (deployment.getEngineVersion() != null) {
          oneDefinition.setEngineVersion(deployment.getEngineVersion());
        }

        if (deployment.getTenantId() != null) {
          oneDefinition.setTenantId(deployment.getTenantId()); // process definition inherits the tenant id
        }
      }

      return new BpmnModelAndProcessDefinitions(bpmnParse.getBpmnModel(), processDefinitions);
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
      return bpmnParse;
    }

    protected boolean isBpmnResource(String resourceName) {
      for (String suffix : BpmnDeployer.BPMN_RESOURCE_SUFFIXES) {
        if (resourceName.endsWith(suffix)) {
          return true;
        }
      }
      return false;
    }
  }
}
