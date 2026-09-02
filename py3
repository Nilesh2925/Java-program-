package com.fincore.commonutilities.util;

import org.springframework.stereotype.Component;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * Utility responsible ONLY for encryption/decryption of sensitive
 * database fields such as EMAIL and PHONE_NUMBER.
 *
 * IMPORTANT:
 * This utility is separate from AesEncryptionUtil.
 *
 * AesEncryptionUtil:
 *     Frontend request/response encryption.
 *
 * DatabaseEncryptionUtil:
 *     Permanent encryption of sensitive values stored in database.
 */
@Component
public class DatabaseEncryptionUtil {

    private static final String ALGORITHM = "AES/GCM/NoPadding";

    // AES-GCM authentication tag = 128 bits = 16 bytes.
    private static final int TAG_LENGTH_BIT = 128;

    // AES-256 requires a 256-bit key = 32 bytes.
    private static final int KEY_LENGTH_BYTES = 32;

    // Recommended GCM IV size.
    private static final int IV_LENGTH_BYTES = 12;

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    /*
     * IMPORTANT:
     *
     * Do NOT hard-code the production encryption key here.
     *
     * This value should ultimately come from environment/secret management.
     */
    private final String base64Key;

    public DatabaseEncryptionUtil() {

        String key = System.getenv("DB_ENCRYPTION_KEY");

        if (key == null || key.isBlank()) {
            throw new IllegalStateException(
                    "DB_ENCRYPTION_KEY environment variable is not configured."
            );
        }

        this.base64Key = key;
    }

    /**
     * Encrypts a plaintext database value using AES-256-GCM.
     *
     * Stored format:
     *
     * Base64(IV):Base64(CipherText)
     *
     * A new random IV is generated for every encryption.
     */
    public String encrypt(String plainText) {

        if (plainText == null) {
            return null;
        }

        if (plainText.isBlank()) {
            return plainText;
        }

        try {
            byte[] keyBytes = Base64.getDecoder().decode(base64Key);

            validateKey(keyBytes);

            byte[] iv = new byte[IV_LENGTH_BYTES];
            SECURE_RANDOM.nextBytes(iv);

            SecretKeySpec secretKey =
                    new SecretKeySpec(keyBytes, "AES");

            GCMParameterSpec gcmParameterSpec =
                    new GCMParameterSpec(TAG_LENGTH_BIT, iv);

            Cipher cipher =
                    Cipher.getInstance(ALGORITHM);

            cipher.init(
                    Cipher.ENCRYPT_MODE,
                    secretKey,
                    gcmParameterSpec
            );

            byte[] encryptedBytes =
                    cipher.doFinal(
                            plainText.getBytes(StandardCharsets.UTF_8)
                    );

            return Base64.getEncoder().encodeToString(iv)
                    + ":"
                    + Base64.getEncoder().encodeToString(encryptedBytes);

        } catch (GeneralSecurityException |
                 IllegalArgumentException e) {

            throw new IllegalStateException(
                    "Unable to encrypt database value.",
                    e
            );
        }
    }

    /**
     * Decrypts a database value encrypted using AES-256-GCM.
     */
    public String decrypt(String encryptedValue) {

        if (encryptedValue == null) {
            return null;
        }

        if (encryptedValue.isBlank()) {
            return encryptedValue;
        }

        try {

            String[] parts = encryptedValue.split(":", 2);

            if (parts.length != 2) {
                throw new IllegalArgumentException(
                        "Invalid encrypted database value."
                );
            }

            byte[] iv =
                    Base64.getDecoder().decode(parts[0]);

            byte[] cipherText =
                    Base64.getDecoder().decode(parts[1]);

            byte[] keyBytes =
                    Base64.getDecoder().decode(base64Key);

            validateKey(keyBytes);

            SecretKeySpec secretKey =
                    new SecretKeySpec(keyBytes, "AES");

            GCMParameterSpec gcmParameterSpec =
                    new GCMParameterSpec(TAG_LENGTH_BIT, iv);

            Cipher cipher =
                    Cipher.getInstance(ALGORITHM);

            cipher.init(
                    Cipher.DECRYPT_MODE,
                    secretKey,
                    gcmParameterSpec
            );

            byte[] decryptedBytes =
                    cipher.doFinal(cipherText);

            return new String(
                    decryptedBytes,
                    StandardCharsets.UTF_8
            );

        } catch (GeneralSecurityException |
                 IllegalArgumentException e) {

            throw new IllegalStateException(
                    "Unable to decrypt database value.",
                    e
            );
        }
    }

    /**
     * Ensures that the configured key is really an AES-256 key.
     */
    private void validateKey(byte[] keyBytes) {

        if (keyBytes.length != KEY_LENGTH_BYTES) {
            throw new IllegalArgumentException(
                    "DB_ENCRYPTION_KEY must contain exactly 32 bytes for AES-256."
            );
        }
    }
}




//converter 

package com.fincore.commonutilities.persistence;

import com.fincore.commonutilities.util.DatabaseEncryptionUtil;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import lombok.RequiredArgsConstructor;

/**
 * JPA converter responsible for transparent encryption/decryption
 * of sensitive String fields.
 *
 * Database write:
 *
 * Java plaintext
 *       ↓
 * encrypt()
 *       ↓
 * encrypted database value
 *
 * Database read:
 *
 * encrypted database value
 *       ↓
 * decrypt()
 *       ↓
 * Java plaintext
 */
@Converter
@RequiredArgsConstructor
public class EncryptedStringConverter
        implements AttributeConverter<String, String> {

    private final DatabaseEncryptionUtil encryptionUtil;

    /**
     * Called automatically by JPA before storing the value in DB.
     */
    @Override
    public String convertToDatabaseColumn(String attribute) {

        if (attribute == null || attribute.isBlank()) {
            return attribute;
        }

        return encryptionUtil.encrypt(attribute);
    }

    /**
     * Called automatically by JPA after reading the value from DB.
     */
    @Override
    public String convertToEntityAttribute(String dbData) {

        if (dbData == null || dbData.isBlank()) {
            return dbData;
        }

        return encryptionUtil.decrypt(dbData);
    }
}




//
package com.tcs.userservice.model;

import com.fincore.commonutilities.persistence.EncryptedStringConverter;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Entity
@Getter
@Setter
@Table(name = "users")
public class User {

    @Id
    @Column(name = "USER_ID")
    private String userId;

    @Column(name = "FIRST_NAME")
    private String firstName;

    @Column(name = "MIDDLE_NAME")
    private String middleName;

    @Column(name = "LAST_NAME")
    private String lastName;

    /*
     * Sensitive field.
     *
     * @Convert tells Hibernate to automatically:
     *
     * Java -> Database : Encrypt
     * Database -> Java : Decrypt
     */
    @Convert(converter = EncryptedStringConverter.class)
    @Column(name = "PHONE_NUMBER")
    private String phoneNumber;

    /*
     * Sensitive field.
     *
     * Email is encrypted before being stored in Oracle.
     */
    @Convert(converter = EncryptedStringConverter.class)
    @Column(name = "EMAIL")
    private String email;

    /*
     * Existing password hash remains unchanged.
     *
     * Password handling should continue using the existing
     * password hashing mechanism.
     */
    @Column(name = "PASSWORD_HASH")
    private String passwordHash;

    @Column(name = "ACCOUNT_STATUS")
    private String accountStatus;

    @Column(name = "CREATED_AT")
    private LocalDateTime createdAt;

    @Column(name = "UPDATED_AT")
    private LocalDateTime updatedAt;

    @Column(name = "LAST_LOGIN_AT")
    private LocalDateTime lastLoginAt;

    @Column(name = "IS_DELETED")
    private char isDeleted;

    @Column(name = "DELETED_AT")
    private LocalDateTime deletedAt;

    @Column(name = "TEMP_PASSWORD_SET_AT")
    private LocalDateTime tempPasswordSetAt;

    @Column(name = "USER_WRONG_PASSWORD_COUNT")
    private int userWrongPasswordCount;

    @Column(name = "BRANCH")
    private int branch;
}



