// Enhanced store-attribution.js with comprehensive bot detection
// Stores attribution data for later lookup during purchases

// Simple in-memory storage (for demo - replace with database in production)
let attributionStore = new Map();

const handler = async (event, context) => {
  // Handle CORS preflight
  if (event.httpMethod === 'OPTIONS') {
    return {
      statusCode: 200,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'POST, GET, OPTIONS'
      }
    };
  }

  if (event.httpMethod === 'POST') {
    try {
      const attributionData = JSON.parse(event.body);
      const userAgent = event.headers['user-agent'] || '';
      const ip = event.headers['x-forwarded-for']?.split(',')[0]?.trim() || 
                 event.headers['x-real-ip'] || 'unknown';
      
      console.log(`🔍 Analyzing traffic from IP: ${ip}`);
      console.log(`🔍 User Agent: ${userAgent}`);
      
      // BOT DETECTION LOGIC
      const botDetection = detectBot(userAgent, attributionData, ip);
      
      if (botDetection.isBot) {
        console.log(`🤖 Bot detected: ${botDetection.reason} - IP: ${ip}`);
        console.log(`🤖 Bot score: ${botDetection.score}/100`);
        
        // Store bot data separately (optional - for analysis)
        const botKey = `bot:${ip}:${Date.now()}`;
        attributionStore.set(botKey, {
          ...attributionData,
          ip_address: ip,
          user_agent: userAgent,
          bot_detected: true,
          bot_reason: botDetection.reason,
          bot_score: botDetection.score,
          stored_at: new Date().toISOString()
        });
        
        return {
          statusCode: 200,
          headers: { 'Access-Control-Allow-Origin': '*' },
          body: JSON.stringify({ 
            success: true, 
            message: 'Bot traffic filtered',
            bot_detected: true,
            reason: botDetection.reason,
            score: botDetection.score
          })
        };
      }
      
      // RATE LIMITING - Check for rapid requests from same IP
      const recentKey = `recent:${ip}`;
      const recentRequests = attributionStore.get(recentKey) || [];
      const now = Date.now();
      const fiveSecondsAgo = now - 5000;
      
      // Filter out requests older than 5 seconds
      const recentValidRequests = recentRequests.filter(time => time > fiveSecondsAgo);
      
      if (recentValidRequests.length >= 2) {
        console.log(`🚫 Rate limited: ${ip} - ${recentValidRequests.length} requests in 5 seconds`);
        return {
          statusCode: 200,
          headers: { 'Access-Control-Allow-Origin': '*' },
          body: JSON.stringify({ 
            success: true, 
            message: 'Rate limited',
            rate_limited: true
          })
        };
      }
      
      // Add current request to recent requests
      recentValidRequests.push(now);
      attributionStore.set(recentKey, recentValidRequests);
      
      // PROCEED WITH NORMAL TRACKING for legitimate traffic
      console.log(`✅ Legitimate traffic detected, storing attribution data`);
      console.log(`✅ Bot score: ${botDetection.score}/100 (below threshold)`);
      
      // Enhanced attribution data with bot detection results
      const enhancedData = {
        ...attributionData,
        ip_address: ip,
        user_agent: userAgent,
        bot_detected: false,
        bot_score: botDetection.score,
        timestamp: new Date().toISOString(),
        is_returning_visitor: checkReturningVisitor(ip),
        session_id: attributionData.session_id || generateSessionId()
      };
      
      // Create multiple lookup keys for flexible matching
      const keys = [
        `ip:${ip}`,
        `session:${enhancedData.session_id}`,
        `timestamp:${Math.floor(new Date().getTime() / 60000)}`
      ];
      
      keys.forEach(key => {
        attributionStore.set(key, {
          ...enhancedData,
          stored_at: new Date().toISOString()
        });
      });
      
      console.log(`✅ Attribution stored with ${keys.length} lookup keys`);
      
      // Send to analytics only if legitimate traffic
      try {
        await fetch('https://trackingojoy.netlify.app/.netlify/functions/analytics', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            ...enhancedData,
            event_type: 'page_view'
          })
        });
        console.log('📊 Legitimate page view sent to analytics');
      } catch (analyticsError) {
        console.log('⚠️ Analytics page view failed:', analyticsError.message);
      }
      
      // Memory management
      if (attributionStore.size > 1000) {
        const entries = Array.from(attributionStore.entries()).slice(-800);
        attributionStore.clear();
        entries.forEach(([key, value]) => attributionStore.set(key, value));
      }
      
      return {
        statusCode: 200,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({ 
          success: true, 
          message: 'Attribution data stored successfully',
          keys: keys.length,
          session_id: enhancedData.session_id,
          bot_score: botDetection.score
        })
      };
      
    } catch (error) {
      console.error('❌ Error storing attribution:', error);
      return {
        statusCode: 500,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({ error: error.message })
      };
    }
  }
  
  if (event.httpMethod === 'GET') {
    // Lookup attribution data
    const { ip, session_id, timestamp, email } = event.queryStringParameters || {};
    
    try {
      let attributionData = null;
      
      // Try different lookup strategies
      if (session_id) {
        attributionData = attributionStore.get(`session:${session_id}`);
        console.log(`🔍 Session lookup for ${session_id}:`, !!attributionData);
      }
      
      if (!attributionData && ip) {
        attributionData = attributionStore.get(`ip:${ip}`);
        console.log(`🔍 IP lookup for ${ip}:`, !!attributionData);
      }
      
      if (!attributionData && timestamp) {
        // Look for attribution data within 5 minutes of the timestamp
        const targetMinute = Math.floor(new Date(timestamp).getTime() / 60000);
        for (let i = -5; i <= 5; i++) {
          const key = `timestamp:${targetMinute + i}`;
          attributionData = attributionStore.get(key);
          if (attributionData) {
            console.log(`🔍 Timestamp lookup found match at ${targetMinute + i}`);
            break;
          }
        }
      }
      
      if (!attributionData) {
        // Fallback: find most recent entry for this IP
        const ipEntries = Array.from(attributionStore.entries())
          .filter(([key]) => key.startsWith(`ip:${ip}`))
          .sort(([,a], [,b]) => new Date(b.timestamp) - new Date(a.timestamp));
        
        if (ipEntries.length > 0) {
          attributionData = ipEntries[0][1];
          console.log(`🔍 Recent IP fallback found data`);
        }
      }
      
      return {
        statusCode: 200,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({ 
          found: !!attributionData,
          data: attributionData,
          lookup_params: { ip, session_id, timestamp, email }
        })
      };
      
    } catch (error) {
      return {
        statusCode: 500,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({ error: error.message })
      };
    }
  }
  
  return {
    statusCode: 405,
    headers: { 'Access-Control-Allow-Origin': '*' },
    body: 'Method not allowed'
  };
};

// COMPREHENSIVE BOT DETECTION FUNCTION
function detectBot(userAgent, attributionData, ip) {
  let score = 0;
  let reasons = [];
  
  const ua = userAgent.toLowerCase();
  
  // Known bot user agents (high confidence) - ENHANCED LIST
  const knownBots = [
    // Search engine bots
    'googlebot', 'bingbot', 'slurp', 'duckduckbot', 'baiduspider',
    'yandexbot', 'sogou', 'exabot',
    
    // Google specific bots
    'adsbot', 'adsbot-google', 'adsbot-google-mobile', 'mediapartners-google',
    'googlebot-mobile', 'googlebot-image', 'googlebot-news', 'googlebot-video',
    'google-adwords-instant', 'google-structured-data-testing-tool',
    
    // Social media bots
    'facebookexternalhit', 'facebookcatalog', 'facebookbot',
    'twitterbot', 'linkedinbot', 'pinterestbot', 'redditbot',
    'whatsapp', 'telegram', 'slackbot', 'discordbot',
    
    // SEO and monitoring bots
    'semrushbot', 'ahrefs', 'ahrefsbot', 'mj12bot', 'dotbot', 'rogerbot',
    'screaming frog', 'seobilitybot', 'serpstatbot', 'linkdexbot',
    'uptimerobot', 'pingdom', 'gtmetrix', 'pagespeed',
    
    // Security and analysis bots
    'netcraftsurveyagent', 'wappalyzer', 'securitytrails',
    'shodan', 'censys', 'masscan', 'nmap',
    
    // Generic bot indicators
    'crawler', 'spider', 'scraper', 'bot', 'crawl', 'fetcher',
    'indexer', 'monitor', 'checker', 'validator', 'test',
    
    // Headless browsers and automation
    'headless', 'phantom', 'selenium', 'puppeteer', 'playwright',
    'chromedriver', 'webdriver', 'automated'
  ];
  
  // Check for known bot patterns
  for (const bot of knownBots) {
    if (ua.includes(bot)) {
      score += 100;
      reasons.push(`Known bot user agent: ${bot}`);
      break;
    }
  }
  
  // Additional bot patterns
  if (ua.includes('compatible;') && ua.includes('+http')) {
    score += 95;
    reasons.push('Bot signature pattern detected');
  }
  
  // Check for Google IP ranges (approximate)
  if (ip.startsWith('66.249.') || ip.startsWith('74.125.') || 
      ip.startsWith('209.85.') || ip.startsWith('216.239.') ||
      ip.startsWith('64.233.') || ip.startsWith('72.14.') ||
      ip.startsWith('216.58.') || ip.startsWith('172.217.')) {
    score += 60;
    reasons.push('Google IP range detected');
  }
  
  // Suspicious user agent patterns
  if (ua.includes('headless') || ua.includes('phantom') || ua.includes('selenium')) {
    score += 90;
    reasons.push('Headless browser detected');
  }
  
  // Empty or very short user agents
  if (!userAgent || userAgent.length < 20) {
    score += 70;
    reasons.push('Suspicious user agent length');
  }
  
  // Very long user agents (sometimes used by bots)
  if (userAgent && userAgent.length > 500) {
    score += 40;
    reasons.push('Unusually long user agent');
  }
  
  // Missing typical browser features
  if (!attributionData.screen_resolution || attributionData.screen_resolution === 'unknown') {
    score += 30;
    reasons.push('Missing screen resolution');
  }
  
  if (!attributionData.language || attributionData.language === 'unknown') {
    score += 20;
    reasons.push('Missing language data');
  }
  
  if (!attributionData.timezone || attributionData.timezone === 'unknown') {
    score += 20;
    reasons.push('Missing timezone data');
  }
  
  // Session duration indicators (if available)
  if (attributionData.session_duration === 0 || attributionData.session_duration < 1000) {
    score += 40;
    reasons.push('Extremely short session duration');
  }
  
  // Suspicious IP patterns
  if (ip === 'unknown' || ip.startsWith('127.') || ip.startsWith('10.') || ip.startsWith('192.168.')) {
    score += 10;
    reasons.push('Local/unknown IP address');
  }
  
  // Check for common bot behaviors
  if (attributionData.pages_viewed === 1 && attributionData.session_duration < 2000) {
    score += 30;
    reasons.push('Single page hit with minimal duration');
  }
  
  // User agent contains version patterns typical of bots
  if (ua.match(/\d+\.\d+\.\d+\.\d+/) && ua.includes('build/')) {
    score += 25;
    reasons.push('Android bot pattern detected');
  }
  
  const isBot = score >= 60; // Threshold for bot detection (lowered slightly)
  
  return {
    isBot,
    score,
    reason: reasons.length > 0 ? reasons.join(', ') : 'Clean traffic detected'
  };
}

// Helper functions
function checkReturningVisitor(ip) {
  const visitorKey = `visitor:${ip}`;
  const exists = attributionStore.has(visitorKey);
  if (!exists) {
    attributionStore.set(visitorKey, Date.now());
  }
  return exists;
}

function generateSessionId() {
  return Date.now().toString(36) + Math.random().toString(36).substr(2);
}

module.exports = { handler };
