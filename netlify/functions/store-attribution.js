// Enhanced store-attribution.js with bot detection
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
      
      // BOT DETECTION LOGIC
      const botDetection = detectBot(userAgent, attributionData, ip);
      
      if (botDetection.isBot) {
        console.log(`🤖 Bot detected: ${botDetection.reason} - IP: ${ip}`);
        
        // Store bot data separately (optional - for analysis)
        const botKey = `bot:${ip}:${Date.now()}`;
        attributionStore.set(botKey, {
          ...attributionData,
          bot_detected: true,
          bot_reason: botDetection.reason,
          stored_at: new Date().toISOString()
        });
        
        return {
          statusCode: 200,
          headers: { 'Access-Control-Allow-Origin': '*' },
          body: JSON.stringify({ 
            success: true, 
            message: 'Bot traffic filtered',
            bot_detected: true,
            reason: botDetection.reason
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
      console.log('✅ Legitimate traffic detected, storing attribution data');
      
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
      
      // Send to analytics only if not a bot
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
  
  // GET requests remain the same as your existing code...
  
  return {
    statusCode: 405,
    headers: { 'Access-Control-Allow-Origin': '*' },
    body: 'Method not allowed'
  };
};

// BOT DETECTION FUNCTION
function detectBot(userAgent, attributionData, ip) {
  let score = 0;
  let reasons = [];
  
  const ua = userAgent.toLowerCase();
  
  // Known bot user agents (high confidence)
  const knownBots = [
    'googlebot', 'bingbot', 'slurp', 'duckduckbot', 'baiduspider',
    'yandexbot', 'sogou', 'facebookexternalhit', 'twitterbot', 'linkedinbot',
    'whatsapp', 'telegram', 'slackbot', 'discordbot', 'applebot',
    'semrushbot', 'ahrefs', 'mj12bot', 'dotbot', 'rogerbot', 'exabot',
    'crawler', 'spider', 'scraper', 'bot', 'crawl'
  ];
  
  for (const bot of knownBots) {
    if (ua.includes(bot)) {
      score += 100;
      reasons.push(`Known bot user agent: ${bot}`);
      break;
    }
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
  
  // Check for missing expected browser features in attribution data
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
  
  // Suspicious IP patterns (optional - implement IP reputation checking)
  if (ip === 'unknown' || ip.startsWith('127.') || ip.startsWith('10.')) {
    score += 10;
    reasons.push('Suspicious IP address');
  }
  
  const isBot = score >= 60; // Threshold for bot detection
  
  return {
    isBot,
    score,
    reason: reasons.join(', ') || 'Legitimate traffic'
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

// In-memory storage
let attributionStore = new Map();

module.exports = { handler };
