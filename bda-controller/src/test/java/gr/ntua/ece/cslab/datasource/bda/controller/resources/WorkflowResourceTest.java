package gr.ntua.ece.cslab.datasource.bda.controller.resources;

import gr.ntua.ece.cslab.datasource.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.datasource.bda.common.storage.beans.Operator;
import gr.ntua.ece.cslab.datasource.bda.common.storage.beans.OperatorParameter;
import gr.ntua.ece.cslab.datasource.bda.common.storage.beans.WorkflowStage;
import gr.ntua.ece.cslab.datasource.bda.common.storage.beans.WorkflowType;
import gr.ntua.ece.cslab.datasource.bda.common.statics.Utils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.security.test.context.support.WithMockUser;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;


@WebMvcTest(WorkflowResource.class)
public class WorkflowResourceTest {


    @Autowired
    private MockMvc mockMvc;

    private MockedStatic<WorkflowType> mockedStatic;


    private static List<WorkflowType> workflowTypeList;

    private static WorkflowType fullWorkflowType;

    @BeforeEach
    public void setUp() {

        String name = "TestType";
        String library = "TestLibrary";
        String description = "Workflows related to test offered from test library";
        workflowTypeList = Arrays.asList(new WorkflowType[] {
                new WorkflowType(1, name + "_" + 1, description, library + "_"+ 1, true),
                new WorkflowType(2, name + "_" + 2, description, library + "_"+ 2, true)
        });

        fullWorkflowType = new WorkflowType(1, name + "_1", description, library + "_" +1, true);
        List<WorkflowStage> stages = new ArrayList<>();

        Integer lastOperatorId = 0;
        Integer lastOperatorParameterId = 0;
        for (int i = 0; i < 3; i ++)
        {
            WorkflowStage workflowStage1 = new WorkflowStage(
                    i,
                    name + "_stage_" + i,
                    description + "_stage_" + i,
                    i,
                    1,
                    2,
                    1,
                    true
            );
            List<Operator> operators = new ArrayList<>();
            for (int j = 0 ; j < 3; j++) {
               Operator operator = new Operator(
                       lastOperatorId,
                       name + "_stage_" + i + "_operator_" + j,
                       description + "_stage_" + i + "_operator_" + j,
                       j,
                       i
               );
               lastOperatorId = lastOperatorId + 1;
               List<OperatorParameter> parameters = new ArrayList<>();

               for (int k = 0; k < 4; k++) {
                   OperatorParameter parameter = new OperatorParameter(
                           lastOperatorParameterId,
                           name + "_stage_" + i + "_operator_" + j  + "_parameter_" + k,
                           k < 2 ? "string" : "integer",
                           j,
                           "{\"type\": \"from_list\", \"evaluate\" : {\"choice_list\" : [\"a\", \"b\", \"c\", \"d\"]}}"
                   );
                   lastOperatorParameterId = lastOperatorParameterId + 1;
                   parameters.add(parameter);
               }

               operator.setParameters(parameters);

               operators.add(operator);
            }
            workflowStage1.setOperators(operators);
            stages.add(workflowStage1);
        }
        fullWorkflowType.setStages(stages);

        mockedStatic = Mockito.mockStatic(WorkflowType.class);
        mockedStatic.when(WorkflowType::getWorkflowTypes).thenReturn(workflowTypeList);
        mockedStatic.when(WorkflowType::getWorkflowTypesAsMap).thenCallRealMethod();
        mockedStatic.when(() ->WorkflowType.getWorkflowTypeById(1)).thenReturn(fullWorkflowType);

    }

    @Test
    @WithMockUser
    public void testGetSearchResults() throws SQLException, SystemConnectorException {


        String expectedResponce = new JSONArray(WorkflowType.
                getWorkflowTypesAsMap().
                stream().
                map(JSONObject::new).
                collect(Collectors.toList())).toString();
        try {
            mockMvc.
                    perform(get("/workflows/{slug}/types", "test")).
                    andExpect(status().isOk()).
                    andExpect(content().json(expectedResponce));
            
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Test
    @WithMockUser
    public void testGetWorkflowType() throws SQLException, SystemConnectorException {

        //MockedStatic<WorkflowType> mockedStatic = Mockito.mockStatic(WorkflowType.class);
        //mockedStatic.when(() ->WorkflowType.getWorkflowTypeById(1)).thenReturn(fullWorkflowType);

 //       String expectedResponce = new JSONArray(WorkflowType.
 //               getWorkflowTypesAsMap().
  //              stream().
   //             map(JSONObject::new).
    //            collect(Collectors.toList())).toString();
     //   try {

//            System.out.println(WorkflowType.getWorkflowTypeById(1, ).toString());
       //     mockMvc.
        ///            perform(get("/workflows/{slug}/types/{workflowTypeId}", "test", "1")).
          ///          andExpect(status().isOk());

//        } catch (Exception e) {
  //          throw new RuntimeException(e);
    //    }
    }

    @Test
    public void testTimestamp() {

        System.out.println(Utils.getCurrentLocalTimestamp());
    }
    @AfterEach
    public void close() {
        this.mockedStatic.close();
    }


}