package gr.ntua.ece.cslab.datasource.bda.common.storage.beans;

import gr.ntua.ece.cslab.datasource.bda.common.storage.SystemConnectorException;
import junit.framework.TestCase;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class WorkflowTypeTest extends TestCase {

    private WorkflowType workflowType;

    private static List<WorkflowType> workflowTypeList;
    private static final String name = "TestType";
    private static final String library = "TestLibrary";
    private static final String description = "Workflows related to test offered from test library";

    public void setUp() throws Exception {
        super.setUp();
        workflowType = new WorkflowType(1, name, description, library, true);
        workflowTypeList = Arrays.asList(new WorkflowType[] {
                new WorkflowType(1, name + "_" + 1, description, library + "_"+ 1, true),
                new WorkflowType(2, name + "_" + 2, description, library + "_"+ 2, true)
        });
    }

    public void tearDown() throws Exception {

    }

    public void testToMap() {
        Map<String, Object> mapp = workflowType.toMap();

        assertEquals(workflowType.getId(), mapp.get("id"));
        assertEquals(description, mapp.get("description"));
        assertEquals(library + "/" + name, mapp.get("name"));
    }


    public void testGetWorkflowTypesAsMap() {

        try (MockedStatic<WorkflowType> mockedStatic = Mockito.mockStatic(WorkflowType.class)) {
            mockedStatic.when(WorkflowType::getWorkflowTypes).thenReturn(workflowTypeList);
            mockedStatic.when(WorkflowType::getWorkflowTypesAsMap).thenCallRealMethod();
            List<Map<String, Object>> result = WorkflowType.getWorkflowTypesAsMap();
            assertEquals(2, result.size());
        } catch (SQLException | SystemConnectorException e) {
            System.out.println("ERROR!");
        }

    }
}