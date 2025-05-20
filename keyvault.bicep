@description('The location for all resources.')
param location string = resourceGroup().location

@description('The name of the production key vault')
param prodKeyVaultName string = 'stanleyakvprod'

@description('The name of the DR key vault')
param drKeyVaultName string = 'stanleyakvdr'

@description('The tenant ID of the subscription')
param tenantId string = subscription().tenantId

// Production Key Vault
resource prodKeyVault 'Microsoft.KeyVault/vaults@2023-02-01' = {
  name: prodKeyVaultName
  location: location
  properties: {
    sku: {
      family: 'A'
      name: 'standard'
    }
    tenantId: tenantId
    enableRbacAuthorization: false
    enableSoftDelete: false
    networkAcls: {
      defaultAction: 'Allow'
      bypass: 'AzureServices'

    }
    accessPolicies: [
      {
        tenantId: tenantId
        objectId: 'b04a03e0-6e07-4d55-83b2-7dedeb56c56d'
        permissions: {
          secrets: [
            'all'

          ]
          keys: [
            'all'
          ]
          certificates: [
            'all'
          ]
        }
      }
    ]
  }
}

// DR Key Vault
resource drKeyVault 'Microsoft.KeyVault/vaults@2023-02-01' = {
  name: drKeyVaultName
  location: location
  properties: {
    sku: {
      family: 'A'
      name: 'standard'
    }
    tenantId: tenantId
    enableRbacAuthorization: false
    enableSoftDelete: false
    networkAcls: {
      defaultAction: 'Allow'
      bypass: 'AzureServices'
    }
        accessPolicies: [
      {
        tenantId: tenantId
        objectId: 'b04a03e0-6e07-4d55-83b2-7dedeb56c56d'
        permissions: {
          secrets: [
            'all'
          ]
          keys: [
            'all'
          ]
          certificates: [
            'all'
          ]
        }
      }
    ]
  }
}

// Production Key Vault Secrets
resource prodSecret1 'Microsoft.KeyVault/vaults/secrets@2023-02-01' = {
  parent: prodKeyVault
  name: 'test-secret-1'
  properties: {
    value: 'test-secret-value-1'
    contentType: 'text/plain'
  }
}

resource prodSecret2 'Microsoft.KeyVault/vaults/secrets@2023-02-01' = {
  parent: prodKeyVault
  name: 'test-secret-2'
  properties: {
    value: 'test-secret-value-2'
    contentType: 'text/plain'
  }
}

// Production Key Vault Keys
resource prodKey1 'Microsoft.KeyVault/vaults/keys@2023-02-01' = {
  parent: prodKeyVault
  name: 'test-key-1'
  properties: {
    kty: 'RSA'
    keySize: 2048
    keyOps: [
      'encrypt'
      'decrypt'
      'sign'
      'verify'
      'wrapKey'
      'unwrapKey'
    ]
  }
}

resource prodKey2 'Microsoft.KeyVault/vaults/keys@2023-02-01' = {
  parent: prodKeyVault
  name: 'test-key-2'
  properties: {
    kty: 'RSA'
    keySize: 2048
    keyOps: [
      'encrypt'
      'decrypt'
      'sign'
      'verify'
      'wrapKey'
      'unwrapKey'
    ]
  }
}

// Production Key Vault Certificates
resource prodCert1 'Microsoft.KeyVault/vaults/certificates@2023-02-01' = {
  parent: prodKeyVault
  name: 'test-cert-1'
  properties: {
    secretProperties: {
      contentType: 'application/x-pkcs12'
    }
    x509CertificateProperties: {
      subject: 'CN=test-cert-1'
      validityInMonths: 12
      keyUsage: [
        'digitalSignature'
        'keyEncipherment'
      ]
    }
    issuerParameters: {
      name: 'Self'
    }
  }
}



// Outputs
output prodKeyVaultId string = prodKeyVault.id
output drKeyVaultId string = drKeyVault.id 
