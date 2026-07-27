@Service("PermissionsService")
public class PermissionsServiceImpl implements PermissionsService {

    /**
     * Method call fetching all related permissions based on user role
     * user : logged-in user data and selected role
     *
     * {@code @Author} : V1010939
     * */
    @Override
    public String getPermissions() {
        return "Hello this is a string";
    }
}


package com.tcs.userservice.service;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@EnableScheduling
@RequiredArgsConstructor
public class RbacCacheScheduler {

    private final RbacCacheSyncService rbacCacheSyncService;

    // Executes at 02:00 AM every day in Indian Standard Time
    @Scheduled(cron = "0 0 2 * * ?", zone = "Asia/Kolkata")
    public void nightlyCacheReconciliation() {

        log.info("CRON Initiated: Starting Nightly RBAC Cache Reconciliation...");
        int count = rbacCacheSyncService.synchronizeAllCaches();
        log.info("CRON Success: Nightly RBAC Cache Reconciliation Completed. Roles Processed: {}", count);
    }
}

package com.tcs.userservice.service;

import com.tcs.userservice.model.Role;
import com.tcs.userservice.repository.RoleRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class RbacCacheSyncService {

    private final RoleRepository roleRepository;
    private final PermissionCacheService permissionCacheService;
    private final CacheManager cacheManager;

    /**
     * Core logic shared by both Manual Trigger and Scheduled Job.
     * Returns the count of processed roles for reporting.
     */
    @Transactional(readOnly = true)
    public int synchronizeAllCaches() {
        log.info("Starting RBAC Cache Synchronization...");

        // 1. Fetch all active roles
        List<Role> allRoles = roleRepository.findAll();

        // 2. Rebuild Redis Cache for each role
        for (Role role : allRoles) {
            permissionCacheService.refreshRolePermissions((long) role.getRoleId());
        }

        // 3. Clear Spring Method Caches (user_configs)
        Cache cache = cacheManager.getCache("notification_configs");
        if (cache != null) {
            cache.clear(); // Instantly drops all notification config keys
        }

        log.info("RBAC Cache Synchronization Completed. Roles Processed: {}", allRoles.size());
        return allRoles.size();
    }
}


package com.tcs.userservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tcs.userservice.dto.PermissionEventDto;
import com.tcs.userservice.dto.RolePermissionEventDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.messaging.handler.annotation.Payload;

import java.io.IOException;

@Service
@Slf4j
@RequiredArgsConstructor
public class RbacEventListener {

    private final PermissionCacheService cacheService;
    private final ObjectMapper objectMapper; // Spring's default mapper

    /**
     * Scenario 1: A Role is assigned/unassigned a Permission.
     * Table: ROLE_PERMISSIONS
     */
    @KafkaListener(topics = "fincore.FINCORE.ROLE_PERMISSIONS", groupId = "rbac-cache-group")
    public void handleRolePermissionChange(@Payload(required = false) String message) {
        // 1. Handle Tombstone Record (Cleanup message from Debezium)
        if (message == null) {
            log.debug("Ignored Tombstone record for ROLE_PERMISSIONS");
            return;
        }

        try {
            // Manual Deserialization
            RolePermissionEventDto event = objectMapper.readValue(message, RolePermissionEventDto.class);

            if (event == null || event.getPayload() == null) return;
            String op = event.getPayload().getOp();

            Long roleId = null;

            if ("c".equals(op) || "u".equals(op)) {
                roleId = event.getPayload().getAfter().getRoleId();
            } else if ("d".equals(op)) {
                roleId = event.getPayload().getBefore().getRoleId();
            }
            if (roleId != null) {
                log.info("⚡ Real-time RBAC Update: Role {} permissions changed.", roleId);
                cacheService.refreshRolePermissions(roleId);
            }
        } catch (JsonProcessingException e) {
            log.error("Invalid JSON format in Kafka message while processing role permissions: {}", e.getMessage());
        } catch (NullPointerException e) {
            log.error("Null pointer encountered during role permissions event processing: {}", e.getMessage());
        }
    }

    /**
     * Scenario 2: A Permission definition itself changes (e.g. URL update).
     * Table: PERMISSIONS
     */
    @KafkaListener(topics = "fincore.FINCORE.PERMISSIONS", groupId = "rbac-cache-group")
    public void handlePermissionDefinitionChange(@Payload(required = false) String message) {
        // 1. Handle Tombstone Record
        if (message == null) {
            log.debug("Ignored Tombstone record for PERMISSIONS");
            return;
        }

        try {
            // Manual Deserialization
            PermissionEventDto event = objectMapper.readValue(message, PermissionEventDto.class);
            if (event == null || event.getPayload() == null) return;
            String op = event.getPayload().getOp();
            Long menuId = null;
            if ("c".equals(op) || "u".equals(op)) {
                menuId = event.getPayload().getAfter().getMenuId();
            } else if ("d".equals(op)) {
                menuId = event.getPayload().getBefore().getMenuId();
            }
            if (menuId != null) {
                log.info("⚡ Real-time RBAC Update: Permission Definition {} changed.", menuId);
                cacheService.refreshRolesByPermissionId(menuId);
            }
        } catch (JsonProcessingException e) {
            log.error("Invalid JSON format in Kafka message while processing permissions event: {}", e.getMessage());
        } catch (NullPointerException e) {
            log.error("Null pointer encountered during permissions event processing: {}", e.getMessage());
        }
    }
}


package com.tcs.userservice.service;

import com.tcs.userservice.ResponseVO;
import com.tcs.userservice.dto.RoleRequestCreateDto;
import com.tcs.userservice.model.RoleRequest;
import org.springframework.http.ResponseEntity;

import com.tcs.userservice.dto.PermissionOrderDto;

import java.util.Map;

public interface RoleRequestService {
    ResponseEntity<ResponseVO<RoleRequest>> createNewRoleRequest(RoleRequestCreateDto dto, String userId);

	ResponseEntity getPendingRoleRequests(String userId);

	ResponseEntity acceptOrRejectRoleRequest(Map<String, Object> request, String userId);

	ResponseEntity getAllRoles(Map<String, Object> request);

	ResponseEntity cancelRoleRequest(Map<String, Object> request, String userId);

	ResponseEntity getMyRoleRequests(String userId);

	ResponseEntity getAllPermissions(Integer roleId);

	ResponseEntity savePermissionOrder(PermissionOrderDto payload);

    ResponseEntity forceSyncRbacCache();
}

package com.tcs.userservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tcs.userservice.ResponseVO;
import com.tcs.userservice.dto.*;
import com.tcs.userservice.exception.ResourceNotFoundException;
import com.tcs.userservice.model.*;
import com.tcs.userservice.repository.PermissionsRepository;
import com.tcs.userservice.repository.RolePermissionsRepository;
import com.tcs.userservice.repository.RoleRepository;
import com.tcs.userservice.repository.RoleRequestRepository;
import com.tcs.userservice.utility.ClobUtil;
import com.tcs.userservice.utility.Constant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class RoleRequestServiceImpl implements RoleRequestService {

    // =============== CONFIGURATION CONSTANTS ======
    private static final String EVENT_SOURCE = "USER_SERVICE";
    private static final String REQUEST_TYPE_KEY = "ROLE_MANAGEMENT";
    private static final List<String> HIDDEN_ROLES = List.of("ADMIN");

    private final RoleRequestRepository roleRequestRepository;
    private final RoleRepository roleRepository;
    private final ObjectMapper objectMapper;
    private final RoleService roleService;
    private final PermissionsRepository permissionsRepository;
    private final RolePermissionsRepository rolePermissionsRepository;
    private final CacheManager cacheManager;
    private final PermissionConfigService permissionConfigService;
    private final NotificationWriterService notificationWriterService;
    private final PermissionCacheService permissionCacheService;
    private final RbacCacheSyncService rbacCacheSyncService;

    @Override
    @Transactional
    public ResponseEntity<ResponseVO<RoleRequest>> createNewRoleRequest(RoleRequestCreateDto dto, String userId) {
        // Extract payload for easy access
        RolePayloadDto payload = dto.getRequestPayload();
        String roleName = payload.getRoleName().trim();
        boolean isCreate = Constant.CREATE.equalsIgnoreCase(dto.getRequestType());

        // 1. Business Logic Validation
        validateBusinessRules(isCreate, dto.getTargetRoleId(), roleName);

        // 2. Map to Entity
        RoleRequest roleRequest = new RoleRequest();
        roleRequest.setRequestType(dto.getRequestType());
        roleRequest.setTargetRoleId(isCreate ? roleRequestRepository.getNewRoleIdOnCreation() : dto.getTargetRoleId());
        roleRequest.setRequestorUserId(userId);
        roleRequest.setRequestStatus(Constant.PENDING);
        roleRequest.setRequestDate(LocalDateTime.now(ZoneId.of("Asia/Kolkata")));

        try {
            // Store as JSON string in DB
            roleRequest.setRequestPayload(objectMapper.writeValueAsString(payload));
        } catch (JsonProcessingException e) {
            log.error("JSON Parsing Error", e);
            throw new IllegalArgumentException("Malformed JSON structure in request payload");
        }
  //---
        RoleRequest saved = roleRequestRepository.save(roleRequest);

        // 1. Notification (To Approvers)
        // Msg: "New Role Request (ID: 55) for Role 51 (MODIFY) is pending approval."
        String notifMsg = String.format("New Role Request (ID: %s) for Role %s (%s) is pending approval.", saved.getRequestId(), saved.getTargetRoleId(), saved.getRequestType());

        NotificationConfigDto config = permissionConfigService.getConfig(REQUEST_TYPE_KEY);

        createNotification(saved.getRequestorUserId(), // creator exclusion
                config.getTargetRoles(), config.getTargetUrl(), notifMsg, String.valueOf(saved.getRequestId()));

        return ResponseEntity.status(HttpStatus.CREATED).body(ResponseVO.<RoleRequest>builder().statusCode(HttpStatus.CREATED).message("Request Created").result(saved).build());
    }

    @Override
    public ResponseEntity<ResponseVO<Map<String, Object>>> getPendingRoleRequests(String userId) {
        List<UserRequestProjection> rawList = roleRequestRepository.findPendingRoleRequests(userId);
        return processProjectionList(rawList, "pendingRequests");
    }

    @Override
    public ResponseEntity<ResponseVO<Map<String, Object>>> getMyRoleRequests(String userId) {
        List<UserRequestProjection> rawList = roleRequestRepository.findMyPendingRoleRequests(userId);
        return processProjectionList(rawList, "myRequests");
    }

    @Override
    @Transactional
    public ResponseEntity<ResponseVO<Map<String, Object>>> acceptOrRejectRoleRequest(Map<String, Object> request, String userId) {
        String actionFlag = (String) request.get("actionFlag");
        int requestId = Integer.parseInt(String.valueOf(request.get("requestId")));
        String remarks = (String) request.getOrDefault("remarks", "No Remarks: Rejected by approver");

        RoleRequest roleRequest = roleRequestRepository.findRoleRequestByRequestId(requestId);
        roleRequest.setApproverUserId(userId);
        roleRequest.setApprovalDate(LocalDateTime.now(ZoneId.of("Asia/Kolkata")));

        Map<String, Object> result = new HashMap<>();
        boolean isApproved = Constant.ACCEPT.equalsIgnoreCase(actionFlag);

        performUpdateOperation(result, actionFlag, roleRequest, remarks);

        // Notification (To Requestor)
        // Msg: "Your Role Request (ID: 55) for Role 51 has been ACCEPTED."
        String status = isApproved ? "ACCEPTED" : "REJECTED";
        String notifMsg = String.format("Your Role Request (ID: %s) for Role %s has been %s.", roleRequest.getRequestId(), roleRequest.getTargetRoleId(), status);

        if (!isApproved) {
            notifMsg += " Reason: " + roleRequest.getReasonForRejection();
        }

        createNotification(roleRequest.getRequestorUserId(), null, "/role-management", notifMsg, String.valueOf(roleRequest.getRequestId()));

        return ResponseEntity.ok(ResponseVO.<Map<String, Object>>builder().statusCode(HttpStatus.OK).message((String) result.get(Constant.MESSAGE)).result(result).build());
    }

    @Override
    @Transactional
    public ResponseEntity<ResponseVO<Map<String, Object>>> cancelRoleRequest(Map<String, Object> request, String userId) {
        int requestId = Integer.parseInt(String.valueOf(request.get("requestId")));
        String remarks = request.get("reason") != null ? (String) request.get("reason") : "No Remarks";

        RoleRequest roleRequest = roleRequestRepository.getRoleRequestByRequestId(requestId);
        if (roleRequest == null) {
            throw new ResourceNotFoundException("Request not found with ID: " + requestId);
        }

        if (!roleRequest.getRequestorUserId().equals(userId)) {
            throw new AccessDeniedException("You are not authorized to cancel this request.");
        }

        if (!Constant.PENDING.equals(roleRequest.getRequestStatus())) {
            log.warn("Attempted to cancel a processed request. ID: {}, Status: {}", requestId, roleRequest.getRequestStatus());
            throw new IllegalStateException("Only PENDING requests can be cancelled.");
        }

        // update cancel status
        roleRequest.setRequestStatus(Constant.CANCEL);
        // update creator id as approver for cancel case
        roleRequest.setApproverUserId(userId);
        roleRequest.setApprovalDate(LocalDateTime.now(ZoneId.of("Asia/Kolkata")));
        roleRequest.setReasonForRejection("CANCELLED BY USER: " + remarks);

        roleRequestRepository.save(roleRequest);

        createNotification(roleRequest.getRequestorUserId(), null, "/role-management", "Role Request (ID: " + requestId + ") has been cancelled.", String.valueOf(requestId));

        return ResponseEntity.ok(ResponseVO.<Map<String, Object>>builder().statusCode(HttpStatus.OK).message("Cancelled").result(Map.of("status", true)).build());
    }

    @Override
    public ResponseEntity<ResponseVO<Map<String, Object>>> getAllRoles(Map<String, Object> request) {
        boolean includePermissions = Boolean.parseBoolean(String.valueOf(request.get("permissions")));
        List<RoleDto> roles = roleService.getAllRolesWithPermissions(includePermissions);

        // =============== FILTERING (REMOVE F1/BOG. ADMIN FROM THE ROLES LIST)
        // ==============
        if (roles != null) {
            roles = roles.stream().filter(r -> r.getRoleName() != null && HIDDEN_ROLES.stream().noneMatch(hidden -> hidden.equalsIgnoreCase(r.getRoleName()))).collect(Collectors.toList());
        }

        return ResponseEntity.ok(ResponseVO.<Map<String, Object>>builder().statusCode(HttpStatus.OK).result(Map.of("roles", roles)).build());
    }

    @Override
    public ResponseEntity<ResponseVO<List<PermissionDto>>> getAllPermissions(Integer roleId) {

        // Fetch permissions evaluated against the Eligibility Matrix
        List<Permissions> permissions = permissionsRepository.findEligiblePermissions(roleId);

        List<PermissionDto> dtos = permissions.stream().map(p ->
                        PermissionDto.builder()
                                .id(p.getMenuId())
                                .title(p.getMenuTitle())
                                .icon(p.getMenuIcon())
                                .menuSubmenu(p.getMenuSubmenu())
                                .description(p.getMenuDescription())
                                .build())
                .collect(Collectors.toList());

        return ResponseEntity.ok(ResponseVO.<List<PermissionDto>>builder().statusCode(HttpStatus.OK).result(dtos).build());
    }

    /**
     * Saves the display order of permissions for a given role.
     * <p>
     * Note: this is a direct write, no notifications triggered from here.
     */
    @Override
    public ResponseEntity<?> savePermissionOrder(PermissionOrderDto payload) {
        // 1. Logic Check (Business Validation)
        if (payload.getPermissions() == null || payload.getPermissions().isEmpty()) {
            throw new IllegalArgumentException("No permissions provided in the request.");
        }

        // 2. Mapping logic
        List<RolePermissions> newPermissions = payload.getPermissions().stream().map(p -> {
            RolePermissions rp = new RolePermissions();
            RolePermissionId newId = new RolePermissionId(payload.getSelectedRole(), p.getId());
            rp.setId(newId);
            rp.setPermissionOrder(p.getOrder());
            return rp;
        }).collect(Collectors.toList());

        // 3. Database Operation
        // Let DataIntegrityViolationException or TransactionSystemException bubble up
        rolePermissionsRepository.saveAll(newPermissions);


        // 4. Clean Success Response
        return ResponseEntity.ok(ResponseVO.<Map<String, Object>>builder().statusCode(HttpStatus.OK).message("Permissions saved successfully").result(Map.of("savedCount", newPermissions.size())).build());
    }

    @Override
    public ResponseEntity<ResponseVO<Map<String, Object>>> forceSyncRbacCache() {
        int rolesProcessed = rbacCacheSyncService.synchronizeAllCaches();

        return ResponseEntity.ok(ResponseVO.<Map<String, Object>>builder()
                .statusCode(HttpStatus.OK)
                .message("RBAC and Config Caches successfully synchronized.")
                .result(Map.of("rolesProcessed", rolesProcessed))
                .build());
    }

    //=========================================== HELPERS =======================================================

    private void performUpdateOperation(Map<String, Object> result, String actionFlag, RoleRequest roleRequest, String remarks) {
        if (actionFlag.equalsIgnoreCase(Constant.ACCEPT)) {
            // Read CLOB payload safely
            String jsonPayload = ClobUtil.clobToString(roleRequest.getRequestPayload());
            RoleRequestPayload payload;
            try {
                payload = objectMapper.readValue(jsonPayload, RoleRequestPayload.class);
            } catch (JsonProcessingException e) {
                log.error("Failed to parse RoleRequest payload for ID: {}", roleRequest.getRequestId());
                throw new IllegalArgumentException("Invalid payload format in request storage.");
            }

            // 1. Get the Reserved ID from the Request
            int roleId = roleRequest.getTargetRoleId();

            Role role = roleRepository.findRoleByRoleId(roleId);

            boolean isCreate = Constant.CREATE.equalsIgnoreCase(roleRequest.getRequestType());

            if (isCreate) {
                if (role != null) {
                    throw new IllegalArgumentException("Role ID " + roleId + " already exists. Cannot Create.");
                }
                role = new Role();
                role.setRoleId(roleId);
            } else {
                if (role == null) {
                    throw new ResourceNotFoundException("Role ID " + roleId + " not found for Update.");
                }
            }

            role.setRoleName(payload.getRoleName());
            role.setDescription(payload.getDescription());
            role.setStatus(Constant.ACTIVE);

            roleRepository.save(role);
            savePermissions(payload, roleId);

            // 1. Evict the notification permissions Cache
            evictPermissionCache(payload.getPermissions());

            // 2. INSTANT REDIS REFRESH
            permissionCacheService.refreshRolePermissions((long) roleId);

            roleRequest.setRequestStatus(Constant.ACCEPTED);
            roleRequest.setExecutionDate(LocalDateTime.now(ZoneId.of("Asia/Kolkata")));
            roleRequest.setExecutionDetails("SUCCESS");
            roleRequestRepository.save(roleRequest);

            result.put(Constant.STATUS, true);
            result.put(Constant.MESSAGE, isCreate ? "Role Created" : "Role Updated");
        } else {
            // REJECT Logic
            roleRequest.setRequestStatus(Constant.REJECTED);
            roleRequest.setReasonForRejection(remarks);
            roleRequest.setExecutionDate(LocalDateTime.now(ZoneId.of("Asia/Kolkata")));
            roleRequest.setExecutionDetails("SUCCESS");
            roleRequestRepository.save(roleRequest);

            result.put(Constant.STATUS, true);
            result.put(Constant.MESSAGE, "Request Rejected");
        }
    }

    private void evictPermissionCache(List<PermissionList> permissions) {
        if (permissions == null || permissions.isEmpty()) return;

        Set<Integer> ids = permissions.stream().map(PermissionList::getId).collect(Collectors.toSet());
        List<String> keys = permissionsRepository.findMappedRequestTypeByMenuId(ids);

        Cache cache = cacheManager.getCache("notification_configs");
        if (cache != null && keys != null) {
            keys.forEach(cache::evict);
        }
    }


    private void savePermissions(RoleRequestPayload payload, int roleId) {
        List<PermissionList> newPerms = payload.getPermissions();
        if (newPerms == null) return;

        // 1. Fetch the authoritative list of what this role is ACTUALLY allowed to have
        List<Permissions> eligiblePermissions = permissionsRepository.findEligiblePermissions(roleId);
        Set<Integer> eligibleMenuIds = eligiblePermissions.stream()
                .map(Permissions::getMenuId)
                .collect(Collectors.toSet());

        // 2. Cross-check incoming payload against the allowed list
        for (PermissionList incomingPerm : newPerms) {
            if (!eligibleMenuIds.contains(incomingPerm.getId())) {
                log.error("SECURITY VIOLATION: Attempted to assign restricted/invalid MENU_ID {} to ROLE_ID {}", incomingPerm.getId(), roleId);
                throw new SecurityException("Payload contains restricted permissions not authorized for this role.");
            }
        }

        // 3. If validation passes, proceed with saving
        List<RolePermissions> current = rolePermissionsRepository.findByIdRoleId(roleId);
        rolePermissionsRepository.deleteAll(current);
        rolePermissionsRepository.flush();

        List<RolePermissions> toSave = newPerms.stream().map(p -> {
            RolePermissions rp = new RolePermissions();
            rp.setId(new RolePermissionId(roleId, p.getId()));
            rp.setPermissionOrder(p.getOrder());
            return rp;
        }).collect(Collectors.toList());

        rolePermissionsRepository.saveAll(toSave);
    }

    private void createNotification(String targetUser, String roles, String url, String msg, String refId) {
        notificationWriterService.createNotification(targetUser, roles, msg, url, refId, EVENT_SOURCE);
    }

    /**
     * Common processor to convert CLOB to String
     */
    private ResponseEntity<ResponseVO<Map<String, Object>>> processProjectionList(List<UserRequestProjection> rawList, String keyName) {
        List<Map<String, Object>> processedList = rawList.stream().map(req -> {
            Map<String, Object> map = new HashMap<>();
            map.put("requestId", req.getRequestId());
            map.put("requestType", req.getRequestType());
            map.put("requestorUserId", req.getRequestorUserId());
            map.put("targetRoleId", req.getTargetRoleId());
            map.put("requestStatus", req.getRequestStatus());
            map.put("requestDate", formatTimestamp(req.getRequestDate()));
            map.put("requestPayload", ClobUtil.clobToString(req.getRequestPayload()));

            // add approver details
            map.put("approverId", req.getApproverUserId());
            map.put("approvalDate", formatTimestamp(req.getApprovalDate()));
            map.put("rejectionReason", req.getReasonForRejection());
            return map;
        }).collect(Collectors.toList());

        Map<String, Object> result = new HashMap<>();
        result.put(keyName, processedList);
        result.put("count", processedList.size());

        return ResponseEntity.ok(ResponseVO.<Map<String, Object>>builder().statusCode(HttpStatus.OK).message("Fetched " + processedList.size() + " requests").result(result).build());
    }
/**
     * Executes Database-dependent business validations (Conflicts, existences, etc.)
     */
    private void validateBusinessRules(boolean isCreate, Integer roleId, String roleName) {
        if (isCreate) {
            if (roleId != null) {
                Role existingRole = roleRepository.findRoleByRoleId(roleId);
                if (existingRole != null) {
                    throw new IllegalArgumentException("Role ID " + roleId + " already exists.");
                }
            }
            // Check Master Table
            if (roleRepository.existsByRoleNameIgnoreCase(roleName.trim())) {
                throw new IllegalArgumentException("Role Name '" + roleName + "' already exists. Please choose a different name.");
            }
            // Check Pending Requests
            if (roleRequestRepository.countPendingRoleRequestsByRoleName(roleName.trim().toLowerCase()) > 0) {
                throw new IllegalArgumentException("The request for Role Name '" + roleName + "' is already pending approval.");
            }
        } else {
            // Modification Validation
            if (roleRequestRepository.countPendingRoleRequests(roleId) > 0) {
                throw new IllegalArgumentException("A pending request already exists for Role ID " + roleId);
            }
        }
    }

    private String formatTimestamp(Timestamp ts) {
        if (ts == null) return null;
        // Use SimpleDateFormat to strictly control output string
        // This produces "2026-01-02T14:14:00.123"
        return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(ts);
    }

}

package com.tcs.userservice.service;

import com.tcs.userservice.dto.PermissionDto;
import com.tcs.userservice.dto.RoleDto;
import com.tcs.userservice.model.Role;
import com.tcs.userservice.repository.RolePermissionRepository;
import com.tcs.userservice.repository.RoleRepository;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;

@Slf4j
@Service
public class RoleService {

	private final RolePermissionRepository rolePermissionRepository;
	private final RoleRepository roleRepository;

	public RoleService(RolePermissionRepository rolePermissionRepository, RoleRepository roleRepository) {
		this.rolePermissionRepository = rolePermissionRepository;
		this.roleRepository = roleRepository;
	}

	public List<RoleDto> getAllRolesWithPermissions(boolean permissions) {

		// Case 1: If permissions flag is false, just return basic role details
		if (!permissions) {
			return roleRepository.findAll().stream()
					.map(role -> RoleDto.builder()
							.roleId(role.getRoleId())
							.roleName(role.getRoleName())
							.roleStatus(role.getStatus())
							.description(role.getDescription()).build())
					.toList();
		}

		// Case 2: If permissions flag is true, return roles with permission details
		List<Object[]> rows = rolePermissionRepository.findAllRolesWithPermissionsRaw();
		Map<Integer, RoleDto> roleMap = new LinkedHashMap<>();

		for (Object[] row : rows) {
			Integer roleId = safeNumberToInteger(row[0]);
			String roleName = safeToString(row[1]);
			String description = safeToString(row[2]);
			String roleStatus = safeToString(row[3]);

			RoleDto role = roleMap.computeIfAbsent(
					roleId, id -> RoleDto.builder()
					.roleId(id)
					.roleName(roleName)
					.description(description)
					.roleStatus(roleStatus)
					.permissions(new ArrayList<>()).build());

			// Add permission only if present
			if (row[4] != null) {
				Integer menuId = safeNumberToInteger(row[4]);
				String menuTitle = safeToString(row[5]);
				String menuIcon = safeToString(row[6]);
				String menuDescription = safeToString(row[7]);
				int order = safeNumberToInteger(row[8]);
                String menuSubmenu = safeToString(row[9]);

                PermissionDto permission = PermissionDto.builder()
						.id(menuId)
						.title(menuTitle)
						.icon(menuIcon)
						.description(menuDescription)
                        .order(order)
                        .menuSubmenu(menuSubmenu)
						.build();

				role.getPermissions().add(permission);
			}
		}

		return new ArrayList<>(roleMap.values());
	}

	private static Integer safeNumberToInteger(Object o) {
		if (o == null)
			return null;
		if (o instanceof Number)
			return ((Number) o).intValue();
		try {
			return Integer.parseInt(o.toString());
		} catch (NumberFormatException e) {
			return null;
		}
	}

	private static String safeToString(Object o) {
		return o == null ? null : o.toString();
	}
}


package com.tcs.userservice.service;

import com.tcs.userservice.ResponseVO;
import com.tcs.userservice.dto.UserRequestCreateDto;
import com.tcs.userservice.dto.UserRequestDto;
import org.springframework.http.ResponseEntity;

import java.util.Map;

public interface UserRequestService {

        ResponseEntity<ResponseVO<Map<String, Object>>> createNewRequest(UserRequestCreateDto dto, String userId);

        ResponseEntity  getPendingRequests(Map<String,Object> request, String userId);

        ResponseEntity acceptOrRejectUserRequest(UserRequestDto userRequestDto,String ipAddress, String userId);

        ResponseEntity getUserDetails(Map<String,String> params);

        ResponseEntity getMyRequests(Map<String,String> params, String userId);

        ResponseEntity cancelRequest(Map<String, Object> request, String userId);

        ResponseEntity validateUserId(String userId);


}


package com.tcs.userservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tcs.userservice.ResponseVO;
import com.tcs.userservice.dto.*;
import com.tcs.userservice.exception.ResourceNotFoundException;
import com.tcs.userservice.model.User;
import com.tcs.userservice.model.UserRequest;
import com.tcs.userservice.model.UserRole;
import com.tcs.userservice.repository.RoleRepository;
import com.tcs.userservice.repository.UserRepository;
import com.tcs.userservice.repository.UserRequestRepository;
import com.tcs.userservice.repository.UserRoleRepository;
import com.tcs.userservice.utility.ClobUtil;
import com.tcs.userservice.utility.Constant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserRequestServiceImpl implements UserRequestService {

    private static final String EVENT_SOURCE = "USER_SERVICE";
    private static final String REQUEST_TYPE_KEY = "USER_MANAGEMENT";
    private static final int RESTRICTED_F1_ROLE_ID = 11;
    private final UserRequestRepository userRequestRepository;
    private final AdAuthenticationService adAuthenticationService;
    private final UserRepository userRepository;
    private final UserRoleRepository userRoleRepository;
    private final ObjectMapper objectMapper;
    private final NotificationWriterService notificationWriterService;
    private final PermissionConfigService permissionConfigService;
    private final RoleRepository roleRepository;

    @Override
    @Transactional
    public ResponseEntity<ResponseVO<Map<String, Object>>> createNewRequest(UserRequestCreateDto dto, String userId) {
        String targetUserId = dto.getTargetUserId();
        String requestType = dto.getRequestType();
        UserPayloadDto payload = dto.getRequestPayload();

        // 1. Mandatory & Format Validations
        if (targetUserId == null || targetUserId.trim().isEmpty()) {
            throw new IllegalArgumentException("User Id is mandatory");
        }

        if (isInvalidUserId(targetUserId)) {
            throw new IllegalArgumentException("Invalid User Id format.");
        }
//
//        // 2. AD Server Checks
//        if (adAuthenticationService.checkIfUserAbsent(targetUserId)) {
//            throw new IllegalStateException("User ID does not exist in AD Server!"); // Map to 409 or 400
//        }

        // 3. Conflict & State Checks
        if (userRequestRepository.countUserPendingRequests(targetUserId) > 0) {
            throw new IllegalStateException("A pending request already exists for this User ID.");
        }

        User existingUser = userRepository.findUserByUserId(targetUserId);

        // Case: Create but User already exists
        if (Constant.CREATE.equalsIgnoreCase(requestType) && existingUser != null) {
            throw new IllegalStateException("User already exists in the system.");
        }

        // Case: Action on non-existent user (Modify/Lock/Delete/etc)
        if (!Constant.CREATE.equalsIgnoreCase(requestType) && existingUser == null) {
            throw new ResourceNotFoundException("User does not exist.");
        }

        // Case: Delete validation from old code
        if (Constant.DELETE.equalsIgnoreCase(requestType) && existingUser != null && "Y".equalsIgnoreCase(String.valueOf(existingUser.getIsDeleted()))) {
            throw new IllegalStateException("User is already deleted.");
        }

//        // 4. AD Email Match Check
//        if (adAuthenticationService.checkIfUserEmailInvalid(targetUserId, payload.getEmail())) {
//            throw new IllegalStateException("User ID and Email combination mismatch with AD Server!");
//        }

        // 5. Role Validation
        int roleId = payload.getRoleId();
        if (!roleRepository.existsById(roleId)) {
            throw new ResourceNotFoundException("Role does not exist in the database.");
        }

        // 6. Advanced Business Logic (F1/BOG Constraints)
        validateF1BogConstraints(requestType, payload, existingUser);

        // 7. Entity Creation & Persistence
        UserRequest request = new UserRequest();
        request.setRequestType(requestType);
        request.setTargetUserId(targetUserId);
        request.setRequestorUserId(userId);
        request.setRequestStatus(Constant.PENDING);
        request.setRequestDate(LocalDateTime.now(ZoneId.of("Asia/Kolkata")));
        try {
            request.setRequestPayload(objectMapper.writeValueAsString(payload));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Error processing payload");
        }

        UserRequest saved = userRequestRepository.save(request);

        // 8. Notification Logic


