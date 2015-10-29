package org.activiti.engine.impl.bpmn.deployer;

import org.activiti.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.activiti.engine.impl.context.Context;
import org.activiti.engine.impl.persistence.entity.ProcessDefinitionEntityManager;
import org.activiti.engine.impl.persistence.entity.ResourceEntityManager;

/**
 * Common base class for classes which may need to access entity managers and/or process engine configuration.
 * This allows the entity managers and/or configuration to be retrieved from the standard Activiti static
 * methods, or, if an alternative implementation prefers to set and reuse them without involving the
 * static paths, for them to be set and retrieved directly.
 */
public class EntityAndConfigurationUser {
  private ProcessEngineConfigurationImpl processEngineConfiguration;
  private ResourceEntityManager resourceEntityManager;
  private ProcessDefinitionEntityManager processDefinitionEntityManager;

  protected ProcessEngineConfigurationImpl getProcessEngineConfiguration() {
    return (processEngineConfiguration == null ?
      Context.getProcessEngineConfiguration() :
      processEngineConfiguration);
  }
  
  protected ResourceEntityManager getResourceEntityManager() {
    return (resourceEntityManager == null ?
      getProcessEngineConfiguration().getResourceEntityManager() :
        resourceEntityManager);
  }
  
  protected ProcessDefinitionEntityManager getProcessDefinitionEntityManager() {
    return (processDefinitionEntityManager == null ? 
        getProcessEngineConfiguration().getProcessDefinitionEntityManager() :
          processDefinitionEntityManager);
  }
}

