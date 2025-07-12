// IPv6 Diagnostic Test
// File: test-ipv6-extraction.js
// Purpose: Test if IPv6 addresses are being filtered during extraction

const headers = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS'
};

exports.handler = async (event, context) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers };
  }

  const redis = (path) => {
    const url = `${process.env.UPSTASH_REDIS_REST_URL}/${path}`;
    return fetch(url, {
      headers: { 'Authorization': `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}` }
    }).then(r => r.json());
  };

  try {
    console.log('🧪 IPv6 DIAGNOSTIC TEST: Checking pageview extraction');
    
    // Test 1: Check for IPv6 patterns in Redis keys
    console.log('\n📊 TEST 1: Scanning for IPv6 Redis keys...');
    const ipv6KeyPatterns = [
      'attribution_ip_2*',  // IPv6 addresses starting with 2001, 2600, etc.
      'attribution_ip_fe80*', // Link-local IPv6
      'attribution_*'       // All attribution keys
    ];
    
    const keyResults = {};
    for (const pattern of ipv6KeyPatterns) {
      let cursor = '0';
      let keys = [];
      let iterations = 0;
      
      do {
        const scanResult = await redis(`scan/${cursor}/match/${pattern}/count/100`);
        if (scanResult.result && scanResult.result[1]) {
          cursor = scanResult.result[0];
          keys = keys.concat(scanResult.result[1]);
          iterations++;
        } else {
          break;
        }
      } while (cursor !== '0' && iterations < 5);
      
      keyResults[pattern] = {
        total_keys: keys.length,
        sample_keys: keys.slice(0, 5),
        ipv6_keys: keys.filter(key => 
          key.includes('_2001_') || key.includes('_2600_') || 
          key.includes('_fe80_') || key.includes('_fc00_')
        ).length
      };
      
      console.log(`   ${pattern}: ${keyResults[pattern].total_keys} total, ${keyResults[pattern].ipv6_keys} IPv6-like`);
    }
    
    // Test 2: Sample actual pageview data and check IP formats
    console.log('\n📊 TEST 2: Sampling pageview data for IP analysis...');
    const sampleKeys = keyResults['attribution_*'].sample_keys;
    const ipAnalysis = {
      ipv4_samples: [],
      ipv6_samples: [],
      invalid_samples: [],
      total_sampled: 0
    };
    
    for (const key of sampleKeys) {
      try {
        const data = await redis(`get/${key}`);
        if (data?.result) {
          const parsed = JSON.parse(data.result);
          ipAnalysis.total_sampled++;
          
          if (parsed.ip_address) {
            const ip = parsed.ip_address;
            
            if (ip.includes(':')) {
              ipAnalysis.ipv6_samples.push({
                ip: ip,
                key: key.substring(0, 30) + '...',
                landing_page: parsed.landing_page?.substring(0, 50)
              });
            } else if (ip.match(/^\d+\.\d+\.\d+\.\d+$/)) {
              ipAnalysis.ipv4_samples.push({
                ip: ip,
                key: key.substring(0, 30) + '...',
                landing_page: parsed.landing_page?.substring(0, 50)
              });
            } else {
              ipAnalysis.invalid_samples.push({
                ip: ip,
                key: key.substring(0, 30) + '...'
              });
            }
          }
        }
      } catch (error) {
        console.log(`   ⚠️ Error sampling ${key}: ${error.message}`);
      }
    }
    
    console.log(`   📊 IP Analysis from ${ipAnalysis.total_sampled} samples:`);
    console.log(`      IPv4: ${ipAnalysis.ipv4_samples.length}`);
    console.log(`      IPv6: ${ipAnalysis.ipv6_samples.length}`);
    console.log(`      Invalid: ${ipAnalysis.invalid_samples.length}`);
    
    // Test 3: Test current extraction validation logic
    console.log('\n📊 TEST 3: Testing IP validation logic...');
    const testIPs = [
      '192.168.1.1',                    // IPv4
      '2001:db8:85a3::8a2e:370:7334',   // IPv6 full
      '2001:db8::1',                    // IPv6 compressed
      'fe80::1',                        // IPv6 link-local
      '::1',                            // IPv6 loopback
      'invalid-ip',                     // Invalid
      '',                               // Empty
      null                              // Null
    ];
    
    const validationResults = testIPs.map(ip => {
      const isValid = validateIP(ip);
      const wouldBeFiltered = !ip || ip === 'unknown' || ip === '';
      
      return {
        ip: ip || 'null',
        is_valid: isValid,
        would_be_filtered: wouldBeFiltered,
        type: ip && ip.includes(':') ? 'IPv6' : 'IPv4'
      };
    });
    
    console.log('   🧪 Validation test results:');
    validationResults.forEach(result => {
      console.log(`      ${result.ip}: Valid=${result.is_valid}, Filtered=${result.would_be_filtered} (${result.type})`);
    });
    
    // Test 4: Simulate extraction filtering
    console.log('\n📊 TEST 4: Simulating extraction filtering...');
    const extractionTest = {
      ipv4_passed: 0,
      ipv6_passed: 0,
      ipv4_filtered: 0,
      ipv6_filtered: 0
    };
    
    [...ipAnalysis.ipv4_samples, ...ipAnalysis.ipv6_samples].forEach(sample => {
      const wouldPass = sample.ip && sample.ip !== 'unknown' && sample.landing_page;
      const isIPv6 = sample.ip.includes(':');
      
      if (wouldPass) {
        if (isIPv6) extractionTest.ipv6_passed++;
        else extractionTest.ipv4_passed++;
      } else {
        if (isIPv6) extractionTest.ipv6_filtered++;
        else extractionTest.ipv4_filtered++;
      }
    });
    
    console.log(`   📊 Extraction simulation:`);
    console.log(`      IPv4 passed: ${extractionTest.ipv4_passed}, filtered: ${extractionTest.ipv4_filtered}`);
    console.log(`      IPv6 passed: ${extractionTest.ipv6_passed}, filtered: ${extractionTest.ipv6_filtered}`);
    
    // Diagnosis
    const diagnosis = {
      ipv6_data_exists: ipAnalysis.ipv6_samples.length > 0,
      ipv6_keys_exist: Object.values(keyResults).some(r => r.ipv6_keys > 0),
      extraction_likely_working: extractionTest.ipv6_passed > 0,
      issue_identified: false,
      recommendations: []
    };
    
    if (!diagnosis.ipv6_data_exists) {
      diagnosis.issue_identified = true;
      diagnosis.recommendations.push('No IPv6 pageviews found in Redis - visitors might genuinely be IPv4-only');
    }
    
    if (diagnosis.ipv6_data_exists && extractionTest.ipv6_passed === 0) {
      diagnosis.issue_identified = true;
      diagnosis.recommendations.push('IPv6 data exists but gets filtered during extraction - validation logic issue');
    }
    
    if (extractionTest.ipv6_filtered > extractionTest.ipv6_passed) {
      diagnosis.issue_identified = true;
      diagnosis.recommendations.push('More IPv6 pageviews filtered than passed - review extraction validation');
    }
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        test_results: {
          redis_key_analysis: keyResults,
          ip_sample_analysis: ipAnalysis,
          validation_test: validationResults,
          extraction_simulation: extractionTest
        },
        diagnosis: diagnosis,
        summary: {
          ipv6_data_in_redis: diagnosis.ipv6_data_exists,
          ipv6_extraction_working: diagnosis.extraction_likely_working,
          issue_found: diagnosis.issue_identified,
          recommendations: diagnosis.recommendations
        }
      })
    };
    
  } catch (error) {
    console.error('❌ IPv6 diagnostic test failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: error.message })
    };
  }
};

// Simple IP validation function for testing
function validateIP(ip) {
  if (!ip || ip === 'unknown' || ip === '') return false;
  
  // IPv4 check
  if (ip.match(/^\d+\.\d+\.\d+\.\d+$/)) {
    const parts = ip.split('.');
    return parts.every(part => {
      const num = parseInt(part, 10);
      return num >= 0 && num <= 255;
    });
  }
  
  // IPv6 check (basic)
  if (ip.includes(':')) {
    return /^[0-9a-fA-F:]+$/.test(ip) && ip.length >= 3 && ip.length <= 45;
  }
  
  return false;
}
