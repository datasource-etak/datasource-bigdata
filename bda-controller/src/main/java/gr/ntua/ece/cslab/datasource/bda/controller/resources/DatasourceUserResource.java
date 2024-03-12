package gr.ntua.ece.cslab.datasource.bda.controller.resources;

import gr.ntua.ece.cslab.datasource.bda.common.Configuration;
import gr.ntua.ece.cslab.datasource.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.datasource.bda.common.storage.beans.DbInfo;
import gr.ntua.ece.cslab.datasource.bda.datastore.StorageBackend;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.DatasourceUser;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.DimensionTable;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.MasterData;
import org.apache.commons.lang.StringUtils;
import org.keycloak.admin.client.CreatedResponseUtil;
import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.KeycloakBuilder;
import org.keycloak.admin.client.resource.*;

import org.keycloak.admin.client.token.TokenManager;
import org.keycloak.representations.AccessTokenResponse;
import org.keycloak.representations.idm.ClientRepresentation;
import org.keycloak.representations.idm.CredentialRepresentation;
import org.keycloak.representations.idm.RoleRepresentation;
import org.keycloak.representations.idm.UserRepresentation;
import org.keycloak.representations.idm.authorization.*;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.ws.rs.core.Response;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class holds the REST API of the datastore object.
 */
@RestController
@CrossOrigin(origins = {"http://10.8.0.6","http://localhost:3000","http://10.8.0.10","http://78.108.34.54"}, allowCredentials = "true")
@RequestMapping("users")
public class DatasourceUserResource {
    private final static Logger LOGGER = Logger.getLogger(DatasourceUserResource.class.getCanonicalName());

    /**
     * Sign up a new user to datasource.
     */
    @PostMapping(value = "signup", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity<?> create(@RequestBody DatasourceUser datasourceUser) {

        if (!StringUtils.isAlphanumeric(datasourceUser.getUsername())) {
            return new ResponseEntity<>("Only alphanumeric characters are allowed in username.", HttpStatus.BAD_REQUEST);
        }

        if (!StringUtils.isAlphanumeric(datasourceUser.getPassword())) {
            return new ResponseEntity<>("Only alphanumeric characters are allowed in password.", HttpStatus.BAD_REQUEST);
        }

        datasourceUser.setUsername(datasourceUser.getUsername().toLowerCase());
        try {
            if (DbInfo.searchForUserspace(datasourceUser.getUsername(), datasourceUser.getEmail()))
                return new ResponseEntity<>("This username has already been used.", HttpStatus.BAD_REQUEST);
        } catch (SQLException e) {
            e.printStackTrace();
            return new ResponseEntity<>("Error in checking user existence.", HttpStatus.INTERNAL_SERVER_ERROR);

        } catch (SystemConnectorException e) {
            e.printStackTrace();
            return new ResponseEntity<>("Error in checking user existence.", HttpStatus.INTERNAL_SERVER_ERROR);
        }


        Configuration configuration = Configuration.getInstance();
        String keycloak_base_url = configuration.securityBackend.getAuthServerUrl();
        String keycloak_master_client = configuration.securityBackend.getMasterClient();
        String keycloak_master_user = configuration.securityBackend.getMasterUser();
        String keycloak_master_pass = configuration.securityBackend.getMasterPass();



        Keycloak keycloak = KeycloakBuilder.builder()
                .serverUrl(keycloak_base_url)
                .realm("master")
                .clientId(keycloak_master_client)
                .grantType("password")
                .username(keycloak_master_user)
                .password(keycloak_master_pass)
                .build();

        UserRepresentation user = new UserRepresentation();
        user.setEnabled(true);
        user.setUsername(datasourceUser.getUsername());
        user.setFirstName(datasourceUser.getFirstName());
        user.setLastName(datasourceUser.getLastName());
        user.setEmail(datasourceUser.getEmail());

        CredentialRepresentation passwordCred = new CredentialRepresentation();
        passwordCred.setTemporary(false);
        passwordCred.setType(CredentialRepresentation.PASSWORD);
        passwordCred.setValue(datasourceUser.getPassword());

        List<CredentialRepresentation> credentials = new LinkedList<>();
        credentials.add(passwordCred);

        user.setCredentials(credentials);



        // Get realm
        RealmResource realmResource = keycloak.realm(configuration.securityBackend.getRealm());
        UsersResource usersRessource = realmResource.users();



        // Create user (requires manage-users role)
        Response response = usersRessource.create(user);
        System.out.printf("Repsonse: %s %s%n", response.getStatus(), response.getStatusInfo());
        System.out.println(response.getLocation());
        String userId = CreatedResponseUtil.getCreatedId(response);
        LOGGER.log(Level.INFO, "User created with userId: %s%n" + userId);



        RolesResource roles = realmResource.roles();
        RoleRepresentation roleRepresentation = new RoleRepresentation();
        roleRepresentation.setName(datasourceUser.getUsername() + "_role");
        roleRepresentation.setClientRole(false);

        roles.create(roleRepresentation);

        for (RoleRepresentation role : roles.list()) {
            if (role.getName().equals(datasourceUser.getUsername() + "_role"))
                roleRepresentation = role;
        }
        String roleId = CreatedResponseUtil.getCreatedId(response);
        LOGGER.log(Level.INFO, "Role created with roleId: %s%n" + roleRepresentation.getId());

        UserResource userResource = usersRessource.get(userId);
        userResource.roles().realmLevel().add(Arrays.asList(roleRepresentation));

        LOGGER.log(Level.INFO, "Role " + roleRepresentation.getId() + " assigned to user " + userId);


        ClientsResource clients = keycloak.realm(configuration.securityBackend.getRealm()).clients();

        ClientRepresentation client =
                clients.findByClientId(configuration.securityBackend.getClientId()).get(0);

        AuthorizationResource authorization =
                clients.get(client.getId()).authorization();

        ResourceRepresentation resourceRepresentation = new ResourceRepresentation();
        resourceRepresentation.setName(datasourceUser.getUsername() + " resource");
        resourceRepresentation.setOwnerManagedAccess(false);
        resourceRepresentation.setDisplayName("DataSource " + datasourceUser.getUsername() + " resource");
        resourceRepresentation.setAttributes(new HashMap<>());
        Set<String> uris = new HashSet();
        /*
        uris.add("/datastore/" + datasourceUser.getUsername());
        uris.add("/kpi/" + datasourceUser.getUsername() + "/**");
        uris.add("/datastore/" + datasourceUser.getUsername() + "/select*");
        uris.add("/datastore/" + datasourceUser.getUsername() + "/entries*");
        uris.add("/datastore/" + datasourceUser.getUsername() + "/");
        uris.add("/datastore/" + datasourceUser.getUsername() + "/dtable*");
        uris.add("/datastore/" + datasourceUser.getUsername() + "/download*");
        uris.add("/datastore/" + datasourceUser.getUsername() + "/search*");
        */
        /*
         * Dataset resource uris
         */
        uris.add("/datasets/" + datasourceUser.getUsername());
        uris.add("/datasets/" + datasourceUser.getUsername() + "/online/search*");
        uris.add("/datasets/" + datasourceUser.getUsername() + "/download*");
        uris.add("/datasets/" + datasourceUser.getUsername() + "/fetch/by-id*");
        uris.add("/datasets/" + datasourceUser.getUsername() + "/fetch/by-alias*");
        uris.add("/datasets/" + datasourceUser.getUsername() + "/description*");
        uris.add("/datasets/" + datasourceUser.getUsername() + "/description/");
        uris.add("/datasets/" + datasourceUser.getUsername() + "/description/by-id*");
        uris.add("/datasets/" + datasourceUser.getUsername() + "/description/by-alias*");
        uris.add("/datasets/" + datasourceUser.getUsername() + "/schema/by-id*");
        uris.add("/datasets/" + datasourceUser.getUsername() + "/join*");
        uris.add("/datasets/" + datasourceUser.getUsername() + "/status/by-id**");
        uris.add("/datasets/" + datasourceUser.getUsername() + "/sourceinfo*");

        uris.add("/workflows/" + datasourceUser.getUsername() + "/types");
        uris.add("/workflows/" + datasourceUser.getUsername() + "/types*");
        uris.add("/workflows/" + datasourceUser.getUsername() + "/**");



        resourceRepresentation.setUris(uris);

        response = authorization.resources().create(resourceRepresentation);
        //System.out.println(Integer.toString(response.getStatus()) + " " + response.getStatusInfo().toString() + " " + response.getEntity().toString());

        LOGGER.log(Level.INFO, "Resource created with userId: %s%n" + CreatedResponseUtil.getCreatedId(response));

        PolicyRepresentation policyRepresentation = new PolicyRepresentation();
        policyRepresentation.setName(datasourceUser.getUsername() + " policy");
        policyRepresentation.setType("role");
        policyRepresentation.setLogic(Logic.POSITIVE);
        policyRepresentation.setDecisionStrategy(DecisionStrategy.UNANIMOUS);
        Map<String, String> policyConfig = new HashMap();
        policyConfig.put("roles", "[{\"id\":\"" + datasourceUser.getUsername() + "_role\",\"required\":false}]");
        policyRepresentation.setConfig(policyConfig);

        response = authorization.policies().create(policyRepresentation);
        //System.out.println(Integer.toString(response.getStatus()) + " " + response.getStatusInfo().toString() + " " + response.getEntity().toString());

        LOGGER.log(Level.INFO, "Policy created with userId: %s%n" + CreatedResponseUtil.getCreatedId(response));

        ResourcePermissionRepresentation permissionRepresentation = new ResourcePermissionRepresentation();
        permissionRepresentation.setName(datasourceUser.getUsername() + " permission");
        permissionRepresentation.setType("resource");
        permissionRepresentation.setLogic(Logic.POSITIVE);
        permissionRepresentation.setDecisionStrategy(DecisionStrategy.UNANIMOUS);
        Set<String> policies = new HashSet<>();
        Set<String> resources = new HashSet<>();
        policies.add(datasourceUser.getUsername() + " policy");
        resources.add(datasourceUser.getUsername() + " resource");
        permissionRepresentation.setPolicies(policies);
        permissionRepresentation.setResources(resources);

        response = authorization.permissions().resource().create(permissionRepresentation);
        //System.out.println(Integer.toString(response.getStatus()) + " " + response.getStatusInfo().toString() + " " + response.getEntity().toString());

        LOGGER.log(Level.INFO, "Permission created with userId: %s%n" + CreatedResponseUtil.getCreatedId(response));


        DbInfo dbInfo = new DbInfo();
        dbInfo.setSlug(datasourceUser.getUsername());
        dbInfo.setName(datasourceUser.getEmail());
        dbInfo.setDescription("DataSource User Space for user " + datasourceUser.getUsername());
        dbInfo.setDbname(datasourceUser.getUsername() + "_db");
        dbInfo.setConnectorId(1);

        DatastoreResource resource = new DatastoreResource();
        ResponseEntity<?> responseAPI = resource.createNewRepository(dbInfo);

        LOGGER.log(Level.INFO, "Userspace creation for user : " + datasourceUser.getUsername());
        LOGGER.log(Level.INFO, "Status : " + responseAPI.getStatusCode());
        LOGGER.log(Level.INFO, "Body : " + responseAPI.getBody());


        DimensionTable dt = new DimensionTable();
        dt.setName("dataset_history_store");
        dt.setSchema(StorageBackend.datasetDTStructure);
        dt.setData(new ArrayList<>());

        DimensionTable dt1 = new DimensionTable();
        dt1.setName("workflow_history_store");
        dt1.setSchema(StorageBackend.workflowDTStructure);
        dt1.setData(new ArrayList<>());

        DimensionTable dt2 = new DimensionTable();
        dt2.setName("operator_history_store");
        dt2.setSchema(StorageBackend.operatorDTStructure);
        dt2.setData(new ArrayList<>());

        MasterData md = new MasterData();
        md.setTables(Arrays.asList(new DimensionTable[]{dt, dt1, dt2}));

        ResponseEntity<?> bootstrapResponseEntity = resource.bootstrap(datasourceUser.getUsername(), md);
        LOGGER.log(Level.INFO, "Initiated dataset log table for user : " + datasourceUser.getUsername());
        LOGGER.log(Level.INFO, "Status : " + responseAPI.getStatusCode());
        LOGGER.log(Level.INFO, "Body : " + responseAPI.getBody());

        return new ResponseEntity<>("User created.", HttpStatus.OK);

    }


    @PostMapping(value = "signin", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity<?> login(@RequestBody DatasourceUser datasourceUser) {

        Configuration.SecurityBackend securityBackend = Configuration.getInstance().securityBackend;

        Keycloak keycloak = KeycloakBuilder.builder()
                .serverUrl(securityBackend.getAuthServerUrl())
                .realm(securityBackend.getRealm())
                .clientId(securityBackend.getClientId())
                .grantType("password")
                .clientSecret(securityBackend.getClientSecret())
                .username(datasourceUser.getUsername())
                .password(datasourceUser.getPassword())
                .build();

        TokenManager tokenManager = keycloak.tokenManager();
        AccessTokenResponse accessToken = tokenManager.getAccessToken();
        return new ResponseEntity<>(accessToken, HttpStatus.OK);
    }

}
