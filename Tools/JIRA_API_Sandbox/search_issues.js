import fetch from 'node-fetch';

const jql = encodeURIComponent('assignee in (membersOf(300IT_GE_SSGZ)) AND (category = BUS OR project = "BKL7 - HEALTHCARE GENERAL (SSGZ)") AND createdDate >= "2025/01/01" AND createdDate <= "2025/08/31" ORDER BY createdDate ASC');

fetch(`http://jira.sm-ms.lan/rest/api/2/search?jql=${jql}`, {
  method: 'GET',
  headers: {
    'Authorization': `Basic ${Buffer.from(
      'email:secret'
    ).toString('base64')}`,
    'Accept': 'application/json'
  }
})
  .then(response => {
    console.log(`Response: ${response.status} ${response.statusText}`);
    return response.text();
  })
  .then(text => console.log(text))
  .catch(err => console.error(err));