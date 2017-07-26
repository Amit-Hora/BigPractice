var restify = require('restify');
var builder = require('botbuilder');

// Setup Restify Server
var server = restify.createServer();
server.listen(process.env.port || process.env.PORT || 3978, function () {
   console.log('%s listening to %s', server.name, server.url); 
});
  
// Create chat connector for communicating with the Bot Framework Service
var connector = new builder.ChatConnector({
    appId: process.env.MICROSOFT_APP_ID,
    appPassword: process.env.MICROSOFT_APP_PASSWORD
});

// Listen for messages from users 
server.post('/api/messages', connector.listen());
// Create your bot with a function to receive messages from the user.
var bot = new builder.UniversalBot(connector, [

    function (session) {
        builder.Prompts.text(session, 'Hi! What is your name?');
    },
    function (session, results) {
        session.send('Hello %s! ', results.response);
         builder.Prompts.choice(session,  "Please confirm your choice","DataBase Access Request Document | Database Creation Request |Contact Jira Admin", {listStyle: builder.ListStyle.button});
        
    },
    
    function (session, results) {
        if('Contact Jira Admin' == results.response.entity){
            session.endConversation('You can contact Amit Hora (hora.amit@bot.com)');

        }else{
session.endConversation('You Selected %s! !', results.response.entity);
        }
        
  
    }
]);


