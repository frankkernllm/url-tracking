exports.handler = async (event, context) => {
  if (event.httpMethod !== 'POST') {
    return {
      statusCode: 405,
      body: JSON.stringify({ error: 'Method not allowed' })
    };
  }

  try {
    const data = JSON.parse(event.body);
    
    const source = data.source || 'direct';
    const campaign = data.campaign || 'none';
    const email = data.email || '';
    const name = data.name || '';
    const timestamp = new Date().toISOString();
    
    console.log('Conversion tracked:', {
      timestamp, source, campaign, email, name
    });
    
    return {
      statusCode: 200,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'POST'
      },
      body: JSON.stringify({ success: true })
    };
    
  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: 'Failed to track' })
    };
  }
};