package org.activiti.engine.impl.bpmn.deployer;

import org.activiti.engine.impl.bpmn.parser.BpmnParse;
import org.activiti.engine.impl.persistence.entity.ProcessDefinitionEntity;
import org.activiti.engine.impl.persistence.entity.ResourceEntity;

import java.util.Collection;
import java.util.Collections;

public class ExpandedBpmnParse {
  /**
   * The BpmnParse set up for a given resource, already executed.
   */
  private final BpmnParse alreadyExecutedParse;
  /**
   * The resource from which this was created.
   */
  private final ResourceEntity resource;

  /**
   * All the process definitions associated with this parse.
   */
  private final Collection<ProcessDefinitionEntity> definitions;
  
  public ExpandedBpmnParse(BpmnParse alreadyExecutedParse, ResourceEntity resource) {
    this.alreadyExecutedParse = alreadyExecutedParse;
    this.resource = resource;

    this.definitions = 
        Collections.unmodifiableList(alreadyExecutedParse.getProcessDefinitions());
  }
  
  public Collection<ProcessDefinitionEntity> getAllProcessDefinitions() {
    return definitions;
  }
  
  public BpmnParse getBpmnParse() {
    return alreadyExecutedParse;
  }
  
  public ResourceEntity getResource() {
    return resource;
  }
}