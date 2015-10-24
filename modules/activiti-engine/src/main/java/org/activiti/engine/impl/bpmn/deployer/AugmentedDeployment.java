package org.activiti.engine.impl.bpmn.deployer;

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
  }

  /**
   * @return
   */
  public Iterable<ProcessDefinitionEntity> getAllProcessDefinitions() {
    // TODO(stm): Auto-generated method stub
    return null;
  }
}

