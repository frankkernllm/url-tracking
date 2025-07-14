// attribution-recovery-diagnostic.js
// SIMPLE DIAGNOSTIC: Test if conversions can match pageviews

const headers = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json'
};

exports.handler = async (event, context) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers };
  }

  const apiKey = event.headers['x-api-key'] || event.headers['X-API-Key'];
  if (apiKey !== process.env.OJOY_API_KEY) {
    return { statusCode: 401, headers, body: JSON.stringify({ error: 'Invalid API key' }) };
  }

  const redis = (cmd) => fetch(`${process.env.UPSTASH_REDIS_REST_URL}/${cmd}`, {
    headers: { Authorization: `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}` }
  }).then(r => r.json());

  try {
    console.log('🔍 DIAGNOSTIC: Testing conversion-to-pageview matching...');

    // Step 1: Get 1 sample conversion
    const conversionSample = await getSampleConversion(redis);
    
    // Step 2: Get 3 sample pageview index keys
    const pageviewSamples = await getSamplePageviewKeys(redis);
    
    // Step 3: Test matching
    const matchTest = testMatching(conversionSample, pageviewSamples);

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        diagnostic_results: {
          conversion_sample: conversionSample,
          pageview_samples: pageviewSamples,
          match_test: matchTest,
          conclusion: matchTest.total_matches > 0 ? 'MATCHING WORKS WITH FIXES' : 'NO MATCHES - DIFFERENT IP RANGES'
        }
      })
    };

  } catch (error) {
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: error.message })
    };
  }
};

async function getSampleConversion(redis) {
  // First, scan for ANY conversion_index_date keys
  const scanResult = await redis('scan/0/match/conversion_index_date:*/count/10');
  const conversionKeys = scanResult?.result?.[1] || [];
  
  if (conversionKeys.length === 0) {
    return { found: false, error: 'No conversion_index_date keys exist', available_keys: [] };
  }
  
  // Try the first available key
  const firstKey = conversionKeys[0];
  const indexData = await redis(`get/${firstKey}`);
  
  if (indexData?.result) {
    const parsed = JSON.parse(decodeURIComponent(indexData.result));
    const sample = parsed.conversions?.[0];
    
    return {
      found: true,
      source_key: firstKey,
      available_keys: conversionKeys,
      total_conversions: parsed.conversion_count,
      sample_conversion: sample ? {
        order_id: sample?.order_id,
        main_ip_address: sample?.main_ip_address,
        winning_ip_value: sample?.winning_ip_value,
        attempted_ip_addresses: sample?.attempted_ip_addresses,
        primary_ip: sample?.primary_ip,
        conversion_ip: sample?.conversion_ip,
        all_ip_fields: Object.keys(sample || {}).filter(k => k.includes('ip'))
      } : null
    };
  }
  
  return { 
    found: false, 
    error: 'Key exists but no data', 
    available_keys: conversionKeys,
    source_key: firstKey 
  };
}

async function getSamplePageviewKeys(redis) {
  const scanResult = await redis('scan/0/match/pageview_index_ip:*/count/5');
  const keys = scanResult?.result?.[1] || [];
  
  return keys.slice(0, 3).map(key => {
    const ipMatch = key.match(/pageview_index_ip:(.+)$/);
    return {
      redis_key: key,
      encoded_ip: ipMatch?.[1],
      decoded_ip: ipMatch?.[1]?.replace(/_/g, ':')
    };
  });
}

function testMatching(conversionSample, pageviewSamples) {
  if (!conversionSample.found) {
    return { error: 'No conversion sample found' };
  }

  const sample = conversionSample.sample_conversion;
  
  // Extract and split comma-separated IPs
  const rawIPs = [
    sample?.main_ip_address,
    sample?.winning_ip_value,
    sample?.primary_ip,
    sample?.conversion_ip
  ].filter(Boolean);
  
  // Split comma-separated strings and flatten
  const allIPs = [];
  rawIPs.forEach(ipField => {
    if (typeof ipField === 'string' && ipField.includes(',')) {
      // Split comma-separated string
      const splitIPs = ipField.split(',').map(ip => ip.trim());
      allIPs.push(...splitIPs);
    } else {
      allIPs.push(ipField);
    }
  });
  
  // Remove duplicates
  const uniqueIPs = [...new Set(allIPs)];
  
  // Test both original and encoded versions for pageview matching
  const pageviewIPs = pageviewSamples.map(p => p.decoded_ip);
  const encodedConversionIPs = uniqueIPs.map(ip => ip.replace(/:/g, '_')); // IPv6 encoding
  
  const originalMatches = uniqueIPs.filter(ip => pageviewIPs.includes(ip));
  const encodedMatches = encodedConversionIPs.filter(encodedIP => {
    // Check if encoded IP exists in pageview samples
    return pageviewSamples.some(p => p.encoded_ip === encodedIP);
  });
  
  return {
    raw_ip_field: sample?.main_ip_address,
    extracted_ips: uniqueIPs,
    encoded_ips: encodedConversionIPs,
    pageview_ips: pageviewIPs,
    original_matches: originalMatches.length,
    encoded_matches: encodedMatches.length,
    total_matches: originalMatches.length + encodedMatches.length,
    matching_ips: [...originalMatches, ...encodedMatches],
    fix_needed: 'Split comma-separated IPs and encode IPv6 with underscores'
  };
}
