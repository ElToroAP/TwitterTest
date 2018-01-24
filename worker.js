var Twitter = require('twitter');
var pg = require('pg');

var tw = new Twitter({
  consumer_key: process.env.TWITTER_CONSUMER_KEY,
  consumer_secret: process.env.TWITTER_CONSUMER_SECRET,
  access_token_key: process.env.TWITTER_ACCESS_TOKEN_KEY,
  access_token_secret: process.env.TWITTER_ACCESS_TOKEN_SECRET,
});

pg.connect(process.env.DATABASE_URL + '?ssl=true', function (err, client, done) {
  if (err) {
    console.error('ELTORO Error Connecting to database: ', err);
    process.exit(1);
  }
  client.query('SELECT twitter_handle__c, sfid ' +
    'FROM salesforce.contact ' +
    'WHERE contact.twitter_handle__c IS NOT NULL', function (err, result) {
      if (err) {
        console.error('ELTORO Error querying contacts: ', err);
      } else {
        console.log('ELTORO: Query: SELECT twitter_handle__c, sfid FROM salesforce.contact WHERE contact.twitter_handle__c IS NOT NULL');
        console.log('ELTORO: Results: ', result);
        var contacts = {};
        result.rows.forEach(function (row) {
          contacts[row.twitter_handle__c.toLowerCase()] = row;
        });
        console.log('ELTORO contacts :', contacts);

        client.query('SELECT hashtag__c, sfid ' +
          'FROM salesforce.campaign ' +
          'WHERE campaign.hashtag__c IS NOT NULL', function (err, result) {
            if (err) {
              console.error('ELTORO Error querying campaings: ', err);
            } else {
              console.log('ELTORO: Query: SELECT hashtag__c, sfid FROM salesforce.campaign WHERE campaign.hashtag__c IS NOT NULL');
              console.log('ELTORO: Results: ', result);
              var campaigns = result.rows;
              var query = '';
              result.rows.forEach(function (row) {
                query += ((query === '') ? '' : ',') + row.hashtag__c;
              });
              console.error('ELTORO query: ', query);
              console.error('ELTORO campaigns: ', campaigns);

              tw.stream('statuses/filter', { track: query }, function (stream) {
                stream.on('data', function (tweet) {
                  console.log('ELTORO Tweet: ', tweet.text.toLowerCase());
                  if (contacts[tweet.user.screen_name.toLowerCase()]) {
                    campaigns.forEach(function (campaign) {
                      if (tweet.text.toLowerCase().indexOf(campaign.hashtag__c.toLowerCase()) !== -1) {
                        console.log('ELTORO: === === ===');
                        console.log('ELTORO: Tweet information: ', tweet);
                        console.log('ELTORO: === === ===');
                        console.log('ELTORO: tweet.id_str: ', tweet.id_str);
                        console.log('ELTORO: tweet.user.screen_name.toLowerCase(): ', tweet.user.screen_name.toLowerCase());
                        console.log('ELTORO: contacts[tweet.user.screen_name.toLowerCase()]: ', contacts[tweet.user.screen_name.toLowerCase()]);
                        console.log('ELTORO: contacts[tweet.user.screen_name.toLowerCase()].sfid: ', contacts[tweet.user.screen_name.toLowerCase()].sfid);
                        console.log('ELTORO: campaign: ', campaign);
                        console.log('ELTORO: campaign.sfid: ', campaign.sfid);
                        console.log('ELTORO: tweet.text: ', tweet.text);
                        console.log('Inserting: ', tweet.id_str, contacts[tweet.user.screen_name.toLowerCase()].sfid, campaign.sfid, tweet.text);
                        var insert = 'INSERT INTO salesforce.tweet__c(name, contact__c, campaign__c, text__c) VALUES($1, $2, $3, $4)';
                        client.query(insert, [tweet.id_str, contacts[tweet.user.screen_name.toLowerCase()].sfid, campaign.sfid, tweet.text], function (err, result) {
                          if (err) {
                            console.error('ELTORO Error inserting into Tweet__c', err);
                          } else {
                            console.log('ELTORO Inserted tweet: ', tweet.id_str);
                          }
                        });
                      } else {
                        console.log('ELTORO: Tweet text [' + tweet.text.toLowerCase() + '] does not contain [' + campaign.hashtag__c.toLowerCase() + ']');
                      }
                    });
                  } else {
                    console.log('ELTORO: Screen_Name [' + tweet.user.screen_name.toLowerCase() + '] is not in contacts: ', contacts);
                  }
                });

                stream.on('error', function (error) {
                  console.error('ELTORO Error fetching from Tweeter: ', err);
                  throw error;
                });
              });
            }
          });
      }
    });
});
