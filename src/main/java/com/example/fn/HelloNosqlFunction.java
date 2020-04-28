package com.example.fn;

import com.fnproject.fn.api.FnConfiguration;
import com.fnproject.fn.api.RuntimeContext;
import com.oracle.bmc.Region;
import com.oracle.bmc.auth.ResourcePrincipalAuthenticationDetailsProvider;
import com.oracle.bmc.nosql.NosqlClient;
import com.oracle.bmc.nosql.model.CreateTableDetails;
import com.oracle.bmc.nosql.model.UpdateRowDetails;
import com.oracle.bmc.nosql.requests.CreateTableRequest;
import com.oracle.bmc.nosql.requests.GetRowRequest;
import com.oracle.bmc.nosql.requests.UpdateRowRequest;
import com.oracle.bmc.nosql.responses.CreateTableResponse;
import com.oracle.bmc.nosql.responses.GetRowResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HelloNosqlFunction {

    private String compartmentId;
    private String regionId;

    @FnConfiguration
    public void config(RuntimeContext ctx) {
        compartmentId = ctx.getConfigurationByKey("COMPARTMENT_ID").orElse("");
        regionId = ctx.getConfigurationByKey("REGION_ID").orElse("");
    }

    /**
     * @return A NosqlClient
     */
    private NosqlClient getNosqlClient() throws IOException {

        System.out.println("Inside HelloNosqlFunction.getNosqlClient()");
        final ResourcePrincipalAuthenticationDetailsProvider provider
                = ResourcePrincipalAuthenticationDetailsProvider.builder().build();

        final NosqlClient nosqlClient = new NosqlClient(provider);
        nosqlClient.setRegion(Region.fromRegionId(regionId));
        return nosqlClient;
    }

    /**
     * Creates a table with two columns: An ID PK and some JSON content
     *
     * @param nosqlClient
     * @throws Exception on Unexpected table creation error
     */
    private void createHelloWorldTable(NosqlClient nosqlClient) throws Exception {

        System.out.println("Inside HelloNosqlFunction.createHelloWorldTable()");
        CreateTableDetails createTableDetails = CreateTableDetails.builder()
                .ddlStatement("CREATE TABLE if not exists hello_world(id LONG, " + "content JSON, primary key (id))")
                .name("hello_world")
                .tableLimits(com.oracle.bmc.nosql.model.TableLimits.builder()
                        .maxReadUnits(25)
                        .maxWriteUnits(25)
                        .maxStorageInGBs(1).build())
                .compartmentId(compartmentId)
                .build();
        CreateTableRequest createTableRequest = CreateTableRequest.builder()
                .createTableDetails(createTableDetails)
                .build();
        CreateTableResponse createTableResponse = nosqlClient.createTable(createTableRequest);
    }

    /**
     * Writes a single record to the "hello_world" table
     *
     * @param nosqlClient
     * @param id          Primary key. If the key exists, the record will be
     *                    overwritten
     * @param jsonContent The content to write
     */
    private void writeOneRecord(NosqlClient nosqlClient, long id, String jsonContent) {

        System.out.println("Inside HelloNosqlFunction.writeOneRecord()");
        Map<String, Object> rowValue = new HashMap<>();
        rowValue.put("id", id);
        rowValue.put("content", jsonContent);
        UpdateRowDetails updateRowDetails = UpdateRowDetails.builder()
                .value(rowValue)
                .compartmentId(compartmentId).build();

        UpdateRowRequest updateRowRequest = UpdateRowRequest.builder()
                .tableNameOrId("hello_world")
                .updateRowDetails(updateRowDetails)
                .build();
        nosqlClient.updateRow(updateRowRequest);
    }

    /**
     * Reads a single record from the "hello_world" table
     *
     * @param nosqlClient
     * @param id          PK of the record to read
     * @return The JSON string containing the content of the requested record or
     * null if the requested record does not exist
     */
    private String readOneRecord(NosqlClient nosqlClient, long id) {

        System.out.println("Inside HelloNosqlFunction.readOneRecord()");
        List<String> keyList = new ArrayList<>();
        keyList.add("id:" + String.valueOf(id));
        GetRowRequest getRowRequest = GetRowRequest.builder()
                .key(keyList)
                .tableNameOrId("hello_world")
                .compartmentId(compartmentId)
                .build();
        GetRowResponse getRowResponse = nosqlClient.getRow(getRowRequest);
        if (getRowResponse != null) {
            return (getRowResponse.getRow().getValue().toString());
        } else {
            return (null);
        }
    }

    public String handleRequest(String input) throws Exception {

        System.out.println("Inside Java HelloNosqlFunction.handleRequest()");
        try {
            NosqlClient nosqlClient = getNosqlClient();
            createHelloWorldTable(nosqlClient);
            writeOneRecord(nosqlClient, 1, "{\"hello\":\"world\"}");
            String readResult = readOneRecord(nosqlClient, 1);
            System.out.println(readResult);
            return readResult;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
