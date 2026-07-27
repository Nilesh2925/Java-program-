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
