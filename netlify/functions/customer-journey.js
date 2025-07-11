// customer-journey.js - Netlify function
// Shows complete customer journey from first visit to conversion

const headers = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Content-Type': 'application/json'
};

exports.handler = async (event, context) => {
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

  const redis = (path) => {
    const url = `${process.env.UPSTASH_REDIS_REST_URL}/${path}`;
    return fetch(url, {
      headers: { 'Authorization': `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}` }
    }).then(r => r.json());
  };

  try {
    const startTime = Date.now();
    const {
      email,
      conversion_timestamp,
      ips_to_check = [],
      session_id,
      device_signature,
      screen_value,
      gpu_signature,
      journey_window_days = 7
    } = JSON.parse(event.body || '{}');

    console.log(`🛣️ CUSTOMER JOURNEY: Mapping journey for ${email}`);
    console.log(`   🕐 Window: ${journey_window_days} days before ${conversion_timestamp}`);
    console.log(`   🔍 Signals: Session=${!!session_id}, IPs=${ips_to_check.length}, Device=${!!device_signature}`);

    const conversionTime = new Date(conversion_timestamp).getTime();
    const windowStart = conversionTime - (journey_window_days * 24 * 60 * 60 * 1000);
    
    // Find ALL pageviews for this customer using multiple attribution signals
    const allPageviews = await findAllCustomerPageviews(redis, {
      ips_to_check,
      session_id,
      device_signature,
      screen_value,
      gpu_signature,
      windowStart,
      conversionTime
    });

    // Build complete customer journey
    const journey = buildCustomerJourney(allPageviews, {
      email,
      conversion_timestamp,
      conversionTime
    });

    const processingTime = Date.now() - startTime;
    console.log(`🏁 Journey mapped: ${journey.pageviews.length} pageviews in ${processingTime}ms`);

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        customer: {
          email,
          conversion_timestamp
        },
        journey: journey,
        summary: {
          total_pageviews: journey.pageviews.length,
          unique_pages: journey.unique_pages.length,
          total_duration_minutes: journey.total_duration_minutes,
          attribution_score: journey.attribution_score,
          first_touch: journey.first_touch,
          last_touch: journey.last_touch
        },
        processing_time_ms: processingTime
      })
    };

  } catch (error) {
    console.error('❌ Customer journey error:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: error.message })
    };
  }
};

// Find ALL pageviews for a customer using comprehensive search
async function findAllCustomerPageviews(redis, searchParams) {
  const { ips_to_check, session_id, device_signature, screen_value, gpu_signature, windowStart, conversionTime } = searchParams;
  
  let allPageviews = [];
  const foundKeys = new Set(); // Prevent duplicates

  console.log('🔍 Comprehensive pageview search for complete customer journey...');

  // STRATEGY: Search by ALL signals simultaneously to find complete journey
  // Don't stop at first match - collect ALL possible pageviews

  // 1. Find by Session ID (highest confidence for journey reconstruction)
  if (session_id) {
    console.log(`   🎯 Session search: ${session_id}`);
    const sessionPageviews = await findPageviewsBySession(redis, session_id, windowStart, conversionTime);
    sessionPageviews.forEach(pv => {
      if (!foundKeys.has(pv._redis_key)) {
        pv.match_method = 'session_id';
        pv.confidence = 300;
        allPageviews.push(pv);
        foundKeys.add(pv._redis_key);
      }
    });
    console.log(`   ✅ Session: ${sessionPageviews.length} pageviews`);
  }

  // 2. Find by Device Signature (cross-session/cross-device tracking)
  if (device_signature) {
    console.log(`   🔐 Device search: ${device_signature}`);
    const devicePageviews = await findPageviewsByDeviceSignature(redis, device_signature, windowStart, conversionTime);
    devicePageviews.forEach(pv => {
      if (!foundKeys.has(pv._redis_key)) {
        pv.match_method = 'device_signature';
        pv.confidence = 220;
        allPageviews.push(pv);
        foundKeys.add(pv._redis_key);
      }
    });
    console.log(`   ✅ Device: ${devicePageviews.length} additional pageviews`);
  }

  // 3. Find by ALL IP Addresses (complete network correlation)
  if (ips_to_check.length > 0) {
    console.log(`   📍 IP search: ${ips_to_check.join(', ')}`);
    for (let i = 0; i < ips_to_check.length; i++) {
      const ip = ips_to_check[i];
      if (!ip || ip === 'unknown') continue;
      
      const ipPageviews = await findPageviewsByIp(redis, ip, windowStart, conversionTime);
      ipPageviews.forEach(pv => {
        if (!foundKeys.has(pv._redis_key)) {
          pv.match_method = i === 0 ? 'primary_ip' : i === 1 ? 'conversion_ip' : 'fallback_ip';
          pv.confidence = i === 0 ? 280 : i === 1 ? 260 : 240;
          allPageviews.push(pv);
          foundKeys.add(pv._redis_key);
        }
      });
      console.log(`   ✅ IP ${ip}: ${ipPageviews.filter(pv => !foundKeys.has(pv._redis_key) || pv.ip_address === ip).length} additional pageviews`);
    }
  }

  // 4. Find by Screen Signature (device consistency tracking)
  if (screen_value) {
    console.log(`   📺 Screen search: ${screen_value}`);
    const screenPageviews = await findPageviewsByScreenSignature(redis, screen_value, windowStart, conversionTime);
    screenPageviews.forEach(pv => {
      if (!foundKeys.has(pv._redis_key)) {
        pv.match_method = 'screen_signature';
        pv.confidence = 200;
        allPageviews.push(pv);
        foundKeys.add(pv._redis_key);
      }
    });
    console.log(`   ✅ Screen: ${screenPageviews.length} additional pageviews`);
  }
  
  // 5. Find by GPU Signature (graphics fingerprint tracking)
  if (gpu_signature) {
    console.log(`   🎮 GPU search: ${gpu_signature}`);
    const gpuPageviews = await findPageviewsByGpuSignature(redis, gpu_signature, windowStart, conversionTime);
    gpuPageviews.forEach(pv => {
      if (!foundKeys.has(pv._redis_key)) {
        pv.match_method = 'webgl_signature';
        pv.confidence = 180;
        allPageviews.push(pv);
        foundKeys.add(pv._redis_key);
      }
    });
    console.log(`   ✅ GPU: ${gpuPageviews.length} additional pageviews`);
  }

  console.log(`🎯 Total unique pageviews found: ${allPageviews.length}`);
  return allPageviews;
}

// Build comprehensive customer journey with enhanced landing page analysis
function buildCustomerJourney(pageviews, conversionInfo) {
  if (pageviews.length === 0) {
    return {
      pageviews: [],
      unique_pages: [],
      landing_page_sequence: [],
      page_flow_analysis: {},
      total_duration_minutes: 0,
      attribution_score: 0,
      first_touch: null,
      last_touch: null,
      journey_events: [],
      chronological_flow: []
    };
  }

  console.log(`🏗️ Building chronological journey from ${pageviews.length} pageviews...`);

  // Sort pageviews chronologically (earliest to latest)
  const sortedPageviews = pageviews.sort((a, b) => 
    new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
  );

  console.log(`📅 Chronological pageview sequence with landing pages:`);
  sortedPageviews.forEach((pv, i) => {
    console.log(`   ${i + 1}. ${new Date(pv.timestamp).toISOString()} - ${pv.landing_page}`);
  });

  // Enhance each pageview with detailed journey and landing page metadata
  const enhancedPageviews = sortedPageviews.map((pv, index) => {
    const pvTime = new Date(pv.timestamp).getTime();
    const conversionTime = new Date(conversionInfo.conversion_timestamp).getTime();
    const nextPv = sortedPageviews[index + 1];
    const prevPv = sortedPageviews[index - 1];
    
    // Calculate time metrics
    const timeToNext = nextPv ? new Date(nextPv.timestamp).getTime() - pvTime : null;
    const timeSincePrevious = prevPv ? pvTime - new Date(prevPv.timestamp).getTime() : null;
    const timeToConversion = conversionTime - pvTime;
    
    // Analyze landing page characteristics
    const landingPageAnalysis = analyzeLandingPage(pv.landing_page);
    
    return {
      ...pv,
      // Journey position and flow
      journey_position: index + 1,
      total_pageviews_in_journey: sortedPageviews.length,
      is_first_touch: index === 0,
      is_last_touch: index === sortedPageviews.length - 1,
      
      // Enhanced landing page data
      landing_page: pv.landing_page,
      landing_page_analysis: landingPageAnalysis,
      page_title: landingPageAnalysis.page_title,
      page_category: landingPageAnalysis.category,
      has_utm_parameters: landingPageAnalysis.has_utm_parameters,
      
      // Time analysis
      time_on_page_minutes: timeToNext ? Math.round(timeToNext / (1000 * 60) * 10) / 10 : null,
      time_since_previous_minutes: timeSincePrevious ? Math.round(timeSincePrevious / (1000 * 60) * 10) / 10 : null,
      time_to_conversion_minutes: Math.round(timeToConversion / (1000 * 60)),
      time_to_conversion_hours: Math.round(timeToConversion / (1000 * 60 * 60) * 10) / 10,
      time_to_conversion_days: Math.round(timeToConversion / (1000 * 60 * 60 * 24) * 10) / 10,
      
      // Page comparison analysis
      is_same_page_as_previous: prevPv ? pv.landing_page === prevPv.landing_page : false,
      is_same_category_as_previous: prevPv ? landingPageAnalysis.category === analyzeLandingPage(prevPv.landing_page).category : false,
      is_same_source_as_previous: prevPv ? (pv.utm_source || pv.source) === (prevPv.utm_source || prevPv.source) : false,
      
      // Attribution method for this specific pageview
      attribution_method: pv.match_method || 'unknown',
      attribution_confidence: pv.confidence || 0
    };
  });

  // Calculate comprehensive journey metrics
  const firstTouch = enhancedPageviews[0];
  const lastTouch = enhancedPageviews[enhancedPageviews.length - 1];
  const journeyStartTime = new Date(firstTouch.timestamp).getTime();
  const journeyEndTime = new Date(lastTouch.timestamp).getTime();
  const conversionTime = new Date(conversionInfo.conversion_timestamp).getTime();
  
  const totalJourneyDuration = journeyEndTime - journeyStartTime;
  const totalJourneyDurationMinutes = Math.round(totalJourneyDuration / (1000 * 60));
  const timeFromLastTouchToConversion = Math.round((conversionTime - journeyEndTime) / (1000 * 60));

  // Enhanced landing page analysis
  const uniquePages = [...new Set(enhancedPageviews.map(pv => pv.landing_page))];
  const landingPageSequence = enhancedPageviews.map(pv => ({
    position: pv.journey_position,
    timestamp: pv.timestamp,
    landing_page: pv.landing_page,
    page_title: pv.page_title,
    category: pv.page_category,
    source: pv.utm_source || pv.source,
    campaign: pv.utm_campaign,
    time_on_page_minutes: pv.time_on_page_minutes
  }));

  // Page frequency and flow analysis
  const pageFrequency = {};
  const pageFlowAnalysis = {
    page_transitions: [],
    category_transitions: [],
    most_visited_pages: {},
    page_categories_visited: new Set()
  };

  enhancedPageviews.forEach((pv, index) => {
    // Page frequency
    pageFrequency[pv.landing_page] = (pageFrequency[pv.landing_page] || 0) + 1;
    
    // Category tracking
    pageFlowAnalysis.page_categories_visited.add(pv.page_category);
    
    // Page transitions
    if (index > 0) {
      const prevPv = enhancedPageviews[index - 1];
      pageFlowAnalysis.page_transitions.push({
        from_page: prevPv.landing_page,
        to_page: pv.landing_page,
        from_category: prevPv.page_category,
        to_category: pv.page_category,
        time_gap_minutes: pv.time_since_previous_minutes
      });
      
      // Category transitions
      if (prevPv.page_category !== pv.page_category) {
        pageFlowAnalysis.category_transitions.push({
          from_category: prevPv.page_category,
          to_category: pv.page_category,
          timestamp: pv.timestamp
        });
      }
    }
  });

  // Most visited pages
  pageFlowAnalysis.most_visited_pages = Object.entries(pageFrequency)
    .sort(([,a], [,b]) => b - a)
    .reduce((obj, [page, count]) => {
      obj[page] = count;
      return obj;
    }, {});

  // Calculate attribution score (highest confidence method found)
  const attributionScore = Math.max(...enhancedPageviews.map(pv => pv.attribution_confidence || 0));

  // Build detailed chronological flow with time gaps
  const chronologicalFlow = buildChronologicalFlow(enhancedPageviews, conversionInfo);

  // Identify all journey events with detailed analysis
  const journeyEvents = identifyDetailedJourneyEvents(enhancedPageviews, conversionInfo);

  return {
    pageviews: enhancedPageviews,
    unique_pages: uniquePages,
    landing_page_sequence: landingPageSequence,
    page_frequency: pageFrequency,
    page_flow_analysis: pageFlowAnalysis,
    total_duration_minutes: totalJourneyDurationMinutes,
    time_from_last_touch_to_conversion_minutes: timeFromLastTouchToConversion,
    attribution_score: attributionScore,
    first_touch: {
      timestamp: firstTouch.timestamp,
      landing_page: firstTouch.landing_page,
      page_title: firstTouch.page_title,
      page_category: firstTouch.page_category,
      source: firstTouch.source || firstTouch.utm_source,
      campaign: firstTouch.utm_campaign,
      medium: firstTouch.utm_medium,
      attribution_method: firstTouch.attribution_method
    },
    last_touch: {
      timestamp: lastTouch.timestamp,
      landing_page: lastTouch.landing_page,
      page_title: lastTouch.page_title,
      page_category: lastTouch.page_category,
      source: lastTouch.source || lastTouch.utm_source,
      campaign: lastTouch.utm_campaign,
      medium: lastTouch.utm_medium,
      attribution_method: lastTouch.attribution_method
    },
    journey_events: journeyEvents,
    chronological_flow: chronologicalFlow,
    journey_insights: {
      is_single_session: enhancedPageviews.every(pv => pv.session_id === firstTouch.session_id),
      has_source_changes: enhancedPageviews.some(pv => pv.is_same_source_as_previous === false),
      has_repeat_visits: Object.values(pageFrequency).some(count => count > 1),
      has_category_changes: pageFlowAnalysis.category_transitions.length > 0,
      page_categories_explored: Array.from(pageFlowAnalysis.page_categories_visited),
      attribution_methods_used: [...new Set(enhancedPageviews.map(pv => pv.attribution_method))],
      journey_complexity: enhancedPageviews.length > 3 ? 'complex' : enhancedPageviews.length > 1 ? 'multi-touch' : 'single-touch'
    }
  };
}

// Analyze landing page characteristics
function analyzeLandingPage(landingPage) {
  if (!landingPage) {
    return {
      page_title: 'Unknown Page',
      category: 'unknown',
      has_utm_parameters: false,
      url_path: '',
      domain: ''
    };
  }

  try {
    const url = new URL(landingPage);
    const pathname = url.pathname;
    const searchParams = new URLSearchParams(url.search);
    
    // Extract page title from URL path
    const pathParts = pathname.split('/').filter(Boolean);
    const lastPart = pathParts[pathParts.length - 1] || 'home';
    const pageTitle = lastPart
      .replace(/-/g, ' ')
      .replace(/\b\w/g, l => l.toUpperCase());
    
    // Categorize page based on URL patterns
    let category = 'general';
    if (pathname.includes('pricing')) category = 'pricing';
    else if (pathname.includes('product')) category = 'product';
    else if (pathname.includes('class') || pathname.includes('course')) category = 'education';
    else if (pathname.includes('branding')) category = 'branding';
    else if (pathname.includes('blog')) category = 'content';
    else if (pathname.includes('about')) category = 'about';
    else if (pathname.includes('contact')) category = 'contact';
    else if (pathname === '/' || pathname === '') category = 'homepage';
    
    // Check for UTM parameters
    const hasUtmParameters = searchParams.has('utm_source') || 
                            searchParams.has('utm_campaign') || 
                            searchParams.has('utm_medium');
    
    return {
      page_title: pageTitle,
      category: category,
      has_utm_parameters: hasUtmParameters,
      url_path: pathname,
      domain: url.hostname,
      query_parameters: Object.fromEntries(searchParams.entries())
    };
    
  } catch (error) {
    // Fallback for invalid URLs
    return {
      page_title: landingPage.split('/').pop() || 'Unknown Page',
      category: 'unknown',
      has_utm_parameters: landingPage.includes('utm_'),
      url_path: landingPage,
      domain: 'unknown'
    };
  }
}

// Build detailed chronological flow with time gaps and transitions
function buildChronologicalFlow(pageviews, conversionInfo) {
  const flow = [];
  
  pageviews.forEach((pv, index) => {
    // Add the pageview event
    flow.push({
      type: 'pageview',
      timestamp: pv.timestamp,
      landing_page: pv.landing_page,
      source: pv.utm_source || pv.source,
      position: index + 1,
      time_on_page_minutes: pv.time_on_page_minutes,
      attribution_method: pv.attribution_method,
      is_first_touch: pv.is_first_touch,
      is_last_touch: pv.is_last_touch
    });
    
    // Add time gap indicator if there's a next pageview
    const nextPv = pageviews[index + 1];
    if (nextPv && pv.time_since_previous_minutes > 60) {
      const gapHours = Math.round(pv.time_since_previous_minutes / 60 * 10) / 10;
      flow.push({
        type: 'time_gap',
        duration_hours: gapHours,
        description: `${gapHours} hour gap between visits`
      });
    }
  });
  
  // Add final conversion event
  const lastPageview = pageviews[pageviews.length - 1];
  const timeGapToConversion = Math.round((new Date(conversionInfo.conversion_timestamp).getTime() - new Date(lastPageview.timestamp).getTime()) / (1000 * 60));
  
  if (timeGapToConversion > 5) {
    flow.push({
      type: 'time_gap',
      duration_minutes: timeGapToConversion,
      description: `${timeGapToConversion} minutes between last touch and conversion`
    });
  }
  
  flow.push({
    type: 'conversion',
    timestamp: conversionInfo.conversion_timestamp,
    email: conversionInfo.email
  });
  
  return flow;
}

// Identify detailed journey events with chronological analysis
function identifyDetailedJourneyEvents(pageviews, conversionInfo) {
  const events = [];
  
  // First touch event
  const firstPv = pageviews[0];
  events.push({
    type: 'first_touch',
    timestamp: firstPv.timestamp,
    description: `First visit from ${firstPv.utm_source || firstPv.source || 'direct'}`,
    landing_page: firstPv.landing_page,
    source: firstPv.utm_source || firstPv.source,
    campaign: firstPv.utm_campaign,
    attribution_method: firstPv.attribution_method,
    details: {
      confidence: firstPv.attribution_confidence,
      session_id: firstPv.session_id,
      device_signature: firstPv.canvas_fingerprint
    }
  });

  // Analyze transitions between pageviews
  for (let i = 1; i < pageviews.length; i++) {
    const prev = pageviews[i - 1];
    const curr = pageviews[i];
    
    const prevSource = prev.utm_source || prev.source;
    const currSource = curr.utm_source || curr.source;
    const timeSincePrevious = curr.time_since_previous_minutes || 0;
    
    // Source change event
    if (prevSource !== currSource) {
      events.push({
        type: 'source_change',
        timestamp: curr.timestamp,
        description: `Source changed: ${prevSource || 'unknown'} → ${currSource || 'unknown'}`,
        from_source: prevSource,
        to_source: currSource,
        time_since_previous_minutes: timeSincePrevious,
        details: {
          from_page: prev.landing_page,
          to_page: curr.landing_page,
          attribution_method: curr.attribution_method
        }
      });
    }
    
    // Long gap between visits (potential re-engagement)
    if (timeSincePrevious > 60) { // More than 1 hour
      events.push({
        type: 'return_visit',
        timestamp: curr.timestamp,
        description: `Returned after ${Math.round(timeSincePrevious / 60 * 10) / 10} hours`,
        gap_hours: Math.round(timeSincePrevious / 60 * 10) / 10,
        details: {
          previous_page: prev.landing_page,
          return_page: curr.landing_page,
          likely_new_session: timeSincePrevious > 240 // 4+ hours
        }
      });
    }
    
    // Same page revisit
    if (prev.landing_page === curr.landing_page) {
      events.push({
        type: 'page_revisit',
        timestamp: curr.timestamp,
        description: `Revisited same page: ${curr.landing_page}`,
        time_since_previous_minutes: timeSincePrevious,
        details: {
          page: curr.landing_page,
          visit_number: pageviews.slice(0, i + 1).filter(pv => pv.landing_page === curr.landing_page).length
        }
      });
    }
  }

  // Last touch event (if different from first)
  if (pageviews.length > 1) {
    const lastPv = pageviews[pageviews.length - 1];
    events.push({
      type: 'last_touch',
      timestamp: lastPv.timestamp,
      description: `Final visit before conversion`,
      landing_page: lastPv.landing_page,
      source: lastPv.utm_source || lastPv.source,
      campaign: lastPv.utm_campaign,
      time_to_conversion_minutes: lastPv.time_to_conversion_minutes,
      details: {
        attribution_method: lastPv.attribution_method,
        confidence: lastPv.attribution_confidence
      }
    });
  }

  // Conversion event
  events.push({
    type: 'conversion',
    timestamp: conversionInfo.conversion_timestamp,
    description: `Conversion completed`,
    email: conversionInfo.email,
    details: {
      total_pageviews: pageviews.length,
      journey_duration_minutes: pageviews[pageviews.length - 1].time_to_conversion_minutes - pageviews[0].time_to_conversion_minutes,
      attribution_score: Math.max(...pageviews.map(pv => pv.attribution_confidence || 0))
    }
  });

  return events;
}

// Get attribution confidence score for pageview
function getAttributionScore(pageview) {
  // Simulate the 8-tier scoring system
  if (pageview.session_id) return 300;
  if (pageview.canvas_fingerprint) return 220;
  if (pageview.ip_address) return 240;
  if (pageview.screen_signature) return 200;
  if (pageview.webgl_fingerprint) return 180;
  return 100; // Geographic/fallback
}

// Individual search functions (simplified versions of multi-signal search)
async function findPageviewsBySession(redis, sessionId, windowStart, conversionTime) {
  // This would need to be enhanced to find ALL pageviews with this session ID
  // Current implementation finds just one - needs to scan all attribution keys
  const pageviews = [];
  
  try {
    // Scan for all attribution records and filter by session ID
    let cursor = '0';
    do {
      const scanResult = await redis(`scan/${cursor}/match/attribution_*/count/1000`);
      if (scanResult.result) {
        cursor = scanResult.result[0];
        const keys = scanResult.result[1] || [];
        
        for (const key of keys) {
          if (key.includes('_session_') || key.includes('_ip_') || key.includes('_fp_')) continue; // Skip index keys
          
          try {
            const pageviewData = await redis(`get/${key}`);
            if (pageviewData.result) {
              const pv = JSON.parse(decodeURIComponent(pageviewData.result));
              const pvTime = new Date(pv.timestamp).getTime();
              
              if (pv.session_id === sessionId && pvTime >= windowStart && pvTime <= conversionTime) {
                pv._redis_key = key;
                pageviews.push(pv);
              }
            }
          } catch (e) {
            // Skip invalid records
          }
        }
      }
    } while (cursor !== '0' && pageviews.length < 100); // Safety limit
    
  } catch (error) {
    console.warn('⚠️ Session search failed:', error);
  }
  
  return pageviews;
}

async function findPageviewsByDeviceSignature(redis, deviceSig, windowStart, conversionTime) {
  // Similar logic to session search but filter by canvas_fingerprint
  const pageviews = [];
  
  try {
    let cursor = '0';
    do {
      const scanResult = await redis(`scan/${cursor}/match/attribution_*/count/1000`);
      if (scanResult.result) {
        cursor = scanResult.result[0];
        const keys = scanResult.result[1] || [];
        
        for (const key of keys) {
          if (key.includes('_session_') || key.includes('_ip_') || key.includes('_fp_')) continue;
          
          try {
            const pageviewData = await redis(`get/${key}`);
            if (pageviewData.result) {
              const pv = JSON.parse(decodeURIComponent(pageviewData.result));
              const pvTime = new Date(pv.timestamp).getTime();
              
              if (pv.canvas_fingerprint === deviceSig && pvTime >= windowStart && pvTime <= conversionTime) {
                pv._redis_key = key;
                pageviews.push(pv);
              }
            }
          } catch (e) {
            // Skip invalid records
          }
        }
      }
    } while (cursor !== '0' && pageviews.length < 100);
    
  } catch (error) {
    console.warn('⚠️ Device signature search failed:', error);
  }
  
  return pageviews;
}

async function findPageviewsByIp(redis, ip, windowStart, conversionTime) {
  // Similar logic but filter by IP address
  const pageviews = [];
  
  try {
    let cursor = '0';
    do {
      const scanResult = await redis(`scan/${cursor}/match/attribution_*/count/1000`);
      if (scanResult.result) {
        cursor = scanResult.result[0];
        const keys = scanResult.result[1] || [];
        
        for (const key of keys) {
          if (key.includes('_session_') || key.includes('_ip_') || key.includes('_fp_')) continue;
          
          try {
            const pageviewData = await redis(`get/${key}`);
            if (pageviewData.result) {
              const pv = JSON.parse(decodeURIComponent(pageviewData.result));
              const pvTime = new Date(pv.timestamp).getTime();
              
              if (pv.ip_address === ip && pvTime >= windowStart && pvTime <= conversionTime) {
                pv._redis_key = key;
                pageviews.push(pv);
              }
            }
          } catch (e) {
            // Skip invalid records
          }
        }
      }
    } while (cursor !== '0' && pageviews.length < 100);
    
  } catch (error) {
    console.warn('⚠️ IP search failed:', error);
  }
  
  return pageviews;
}

async function findPageviewsByScreenSignature(redis, screenValue, windowStart, conversionTime) {
  // Filter by screen signature
  const pageviews = [];
  
  try {
    let cursor = '0';
    do {
      const scanResult = await redis(`scan/${cursor}/match/attribution_*/count/1000`);
      if (scanResult.result) {
        cursor = scanResult.result[0];
        const keys = scanResult.result[1] || [];
        
        for (const key of keys) {
          if (key.includes('_session_') || key.includes('_ip_') || key.includes('_fp_')) continue;
          
          try {
            const pageviewData = await redis(`get/${key}`);
            if (pageviewData.result) {
              const pv = JSON.parse(decodeURIComponent(pageviewData.result));
              const pvTime = new Date(pv.timestamp).getTime();
              
              if (pv.screen_signature === screenValue && pvTime >= windowStart && pvTime <= conversionTime) {
                pv._redis_key = key;
                pageviews.push(pv);
              }
            }
          } catch (e) {
            // Skip invalid records
          }
        }
      }
    } while (cursor !== '0' && pageviews.length < 100);
    
  } catch (error) {
    console.warn('⚠️ Screen signature search failed:', error);
  }
  
  return pageviews;
}

async function findPageviewsByGpuSignature(redis, gpuSig, windowStart, conversionTime) {
  // Filter by WebGL fingerprint
  const pageviews = [];
  
  try {
    let cursor = '0';
    do {
      const scanResult = await redis(`scan/${cursor}/match/attribution_*/count/1000`);
      if (scanResult.result) {
        cursor = scanResult.result[0];
        const keys = scanResult.result[1] || [];
        
        for (const key of keys) {
          if (key.includes('_session_') || key.includes('_ip_') || key.includes('_fp_')) continue;
          
          try {
            const pageviewData = await redis(`get/${key}`);
            if (pageviewData.result) {
              const pv = JSON.parse(decodeURIComponent(pageviewData.result));
              const pvTime = new Date(pv.timestamp).getTime();
              
              if (pv.webgl_fingerprint === gpuSig && pvTime >= windowStart && pvTime <= conversionTime) {
                pv._redis_key = key;
                pageviews.push(pv);
              }
            }
          } catch (e) {
            // Skip invalid records
          }
        }
      }
    } while (cursor !== '0' && pageviews.length < 100);
    
  } catch (error) {
    console.warn('⚠️ GPU signature search failed:', error);
  }
  
  return pageviews;
}
