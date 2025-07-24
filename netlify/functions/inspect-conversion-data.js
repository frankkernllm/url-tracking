// netlify/functions/inspect-conversion-data.js
// PURPOSE: Examine actual conversion data structure to determine correct field names

const headers = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
  'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
  'Content-Type': 'application/json'
};

exports.handler = async (event, context) => {
  context.callbackWaitsForEmptyEventLoop = false;
  
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers };
  }

  // Validate API key
  const apiKey = event.headers['x-api-key'] || event.headers['X-API-Key'];
  if (apiKey !== process.env.OJOY_API_KEY) {
    return {
      statusCode: 401,
      headers,
      body: JSON.stringify({ error: 'Invalid API key' })
    };
  }

  try {
    console.log('🔍 INSPECTING CONVERSION DATA STRUCTURE');
    const startTime = Date.now();
    
    const redis = initializeRedis();
    
    // Get parameters
    const body = event.body ? JSON.parse(event.body) : {};
    const {
      target_order_id = null,
      sample_count = 5
    } = body;
    
    let inspectionResults = {
      conversions_examined: 0,
      field_analysis: {},
      sample_conversions: [],
      target_conversion: null,
      field_name_recommendations: {}
    };
    
    // Scan conversion keys
    let cursor = '0';
    let conversionsFound = 0;
    const maxIterations = 20;
    let iterations = 0;
    
    do {
      const scanResult = await redis(`scan/${cursor}/match/conversions:*/count/100`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      
      console.log(`📊 Examining batch of ${keys.length} conversion keys...`);
      
      // Examine each conversion
      for (const key of keys) {
        try {
          const conversionData = await redis(`get/${key}`, 2000);
          
          if (conversionData?.result) {
            const conversion = JSON.parse(decodeURIComponent(conversionData.result));
            
            conversionsFound++;
            inspectionResults.conversions_examined = conversionsFound;
            
            // Analyze field structure
            analyzeConversionFields(conversion, inspectionResults.field_analysis);
            
            // Check if this is our target conversion
            if (target_order_id && 
                (conversion.order_id == target_order_id || 
                 conversion.conversion_order_id == target_order_id ||
                 conversion.orderID == target_order_id)) {
              
              inspectionResults.target_conversion = {
                redis_key: key,
                all_fields: Object.keys(conversion),
                email_fields: extractEmailFields(conversion),
                order_id_fields: extractOrderIdFields(conversion),
                ip_fields: extractIPFields(conversion),
                full_data: conversion
              };
              
              console.log(`🎯 FOUND TARGET CONVERSION ${target_order_id}:`, inspectionResults.target_conversion.email_fields);
            }
            
            // Collect sample conversions
            if (inspectionResults.sample_conversions.length < sample_count) {
              inspectionResults.sample_conversions.push({
                redis_key: key,
                timestamp: conversion.timestamp || conversion.conversion_timestamp || 'unknown',
                email_fields: extractEmailFields(conversion),
                order_id_fields: extractOrderIdFields(conversion),
                ip_fields: extractIPFields(conversion),
                all_field_names: Object.keys(conversion)
              });
            }
            
            // Stop if we've examined enough samples and found target (if specified)
            if (inspectionResults.sample_conversions.length >= sample_count && 
                (!target_order_id || inspectionResults.target_conversion)) {
              break;
            }
          }
          
        } catch (parseError) {
          console.warn(`⚠️ Failed to parse conversion ${key}:`, parseError.message);
        }
      }
      
      // Stop if we've examined enough samples and found target (if specified)
      if (inspectionResults.sample_conversions.length >= sample_count && 
          (!target_order_id || inspectionResults.target_conversion)) {
        break;
      }
      
      iterations++;
    } while (cursor !== '0' && iterations < maxIterations);
    
    // Generate field name recommendations
    inspectionResults.field_name_recommendations = generateFieldRecommendations(inspectionResults.field_analysis);
    
    const processingTime = Date.now() - startTime;
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        inspection_summary: {
          conversions_examined: inspectionResults.conversions_examined,
          processing_time_ms: processingTime,
          target_order_id: target_order_id,
          target_found: !!inspectionResults.target_conversion
        },
        field_analysis: inspectionResults.field_analysis,
        field_recommendations: inspectionResults.field_name_recommendations,
        sample_conversions: inspectionResults.sample_conversions,
        target_conversion: inspectionResults.target_conversion,
        conclusion: {
          most_common_email_field: getMostCommonField(inspectionResults.field_analysis, 'email'),
          most_common_order_id_field: getMostCommonField(inspectionResults.field_analysis, 'order_id'),
          most_common_ip_field: getMostCommonField(inspectionResults.field_analysis, 'ip')
        }
      })
    };
    
  } catch (error) {
    console.error('❌ Conversion inspection failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Conversion inspection failed', 
        message: error.message
      })
    };
  }
};

// Analyze fields in a conversion record
function analyzeConversionFields(conversion, fieldAnalysis) {
  for (const [fieldName, value] of Object.entries(conversion)) {
    if (!fieldAnalysis[fieldName]) {
      fieldAnalysis[fieldName] = {
        count: 0,
        sample_values: [],
        field_type: 'unknown'
      };
    }
    
    fieldAnalysis[fieldName].count++;
    
    // Collect sample values (max 3)
    if (fieldAnalysis[fieldName].sample_values.length < 3 && value) {
      fieldAnalysis[fieldName].sample_values.push(value);
    }
    
    // Determine field type
    if (fieldName.toLowerCase().includes('email') || 
        (typeof value === 'string' && value.includes('@'))) {
      fieldAnalysis[fieldName].field_type = 'email';
    } else if (fieldName.toLowerCase().includes('order') || 
               fieldName.toLowerCase().includes('id')) {
      fieldAnalysis[fieldName].field_type = 'order_id';
    } else if (fieldName.toLowerCase().includes('ip') || 
               fieldName.toLowerCase().includes('address')) {
      fieldAnalysis[fieldName].field_type = 'ip';
    } else if (fieldName.toLowerCase().includes('timestamp') || 
               fieldName.toLowerCase().includes('time') ||
               fieldName.toLowerCase().includes('date')) {
      fieldAnalysis[fieldName].field_type = 'timestamp';
    }
  }
}

// Extract email-related fields
function extractEmailFields(conversion) {
  const emailFields = {};
  
  for (const [key, value] of Object.entries(conversion)) {
    if (key.toLowerCase().includes('email') || 
        (typeof value === 'string' && value.includes('@'))) {
      emailFields[key] = value;
    }
  }
  
  return emailFields;
}

// Extract order ID related fields
function extractOrderIdFields(conversion) {
  const orderFields = {};
  
  for (const [key, value] of Object.entries(conversion)) {
    if (key.toLowerCase().includes('order') || 
        (key.toLowerCase().includes('id') && typeof value === 'number')) {
      orderFields[key] = value;
    }
  }
  
  return orderFields;
}

// Extract IP related fields
function extractIPFields(conversion) {
  const ipFields = {};
  
  for (const [key, value] of Object.entries(conversion)) {
    if (key.toLowerCase().includes('ip') || 
        key.toLowerCase().includes('address') ||
        (typeof value === 'string' && (value.includes(':') || value.match(/^\d+\.\d+\.\d+\.\d+$/)))) {
      ipFields[key] = value;
    }
  }
  
  return ipFields;
}

// Generate field name recommendations
function generateFieldRecommendations(fieldAnalysis) {
  const recommendations = {
    email_field: null,
    order_id_field: null,
    primary_ip_field: null,
    confidence: 'unknown'
  };
  
  // Find most common email field
  const emailFields = Object.entries(fieldAnalysis)
    .filter(([key, data]) => data.field_type === 'email')
    .sort((a, b) => b[1].count - a[1].count);
  
  if (emailFields.length > 0) {
    recommendations.email_field = emailFields[0][0];
  }
  
  // Find most common order ID field
  const orderFields = Object.entries(fieldAnalysis)
    .filter(([key, data]) => data.field_type === 'order_id')
    .sort((a, b) => b[1].count - a[1].count);
  
  if (orderFields.length > 0) {
    recommendations.order_id_field = orderFields[0][0];
  }
  
  // Find most common IP field
  const ipFields = Object.entries(fieldAnalysis)
    .filter(([key, data]) => data.field_type === 'ip')
    .sort((a, b) => b[1].count - a[1].count);
  
  if (ipFields.length > 0) {
    recommendations.primary_ip_field = ipFields[0][0];
  }
  
  // Determine confidence
  if (recommendations.email_field && recommendations.order_id_field) {
    recommendations.confidence = 'high';
  } else if (recommendations.email_field || recommendations.order_id_field) {
    recommendations.confidence = 'medium';
  } else {
    recommendations.confidence = 'low';
  }
  
  return recommendations;
}

// Get most common field of a type
function getMostCommonField(fieldAnalysis, fieldType) {
  const fieldsOfType = Object.entries(fieldAnalysis)
    .filter(([key, data]) => data.field_type === fieldType)
    .sort((a, b) => b[1].count - a[1].count);
  
  return fieldsOfType.length > 0 ? fieldsOfType[0][0] : null;
}

// Initialize Redis helper
function initializeRedis() {
  const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
  
  return async (command, timeoutMs = 5000) => {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
    
    try {
      const response = await fetch(`${redisUrl}/${command}`, {
        headers: { 
          Authorization: `Bearer ${redisToken}`,
          'Content-Type': 'application/json'
        },
        signal: controller.signal
      });
      
      clearTimeout(timeoutId);
      
      if (!response.ok) {
        throw new Error(`Redis error: ${response.status}`);
      }
      
      return await response.json();
    } catch (error) {
      clearTimeout(timeoutId);
      throw error;
    }
  };
}
