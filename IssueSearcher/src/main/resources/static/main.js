
$(document).ready(function () {

    $("#search-form").submit(function (event) {

        //stop submit the form, we will post it manually.
        event.preventDefault();

        fire_ajax_submit();

    });

});
function getissue() {
	
	
	var search = {}
    search["issue"] = $("#issue").val();
	
	
	$.ajax({
        type: "POST",
        contentType: "application/json",
        url: "/api/search",
        data: JSON.stringify(search),
        dataType: 'json',
        cache: false,
        timeout: 600000,
        success: function (data) {

  //          var json = "<h4>Ajax Response</h4><pre>"
  //              + JSON.stringify(data, null, 4) + "</pre>";
  //          $('#feedback').html(json);

            
        	$('#feedback').html('');

        	var combo = $("<select onchange='getquerytext()' id ='queryoption' size='10' style='text-overflow:ellipsis;'></select>")

            $.each(data, function (i, el) {
            	var elarray=el.split("$%")
                combo.append("<option value = \""+elarray[1]+"\">" + elarray[0] + "</option>");
            });

          //  $("#issuesuggester").append(combo);
        	$('#feedback').html(combo);
            
            console.log("SUCCESS : ", data);
            $("#btn-search").prop("disabled", false);

        },
        error: function (e) {
        

        	//$("#issuesuggester").val(e.responseText );

            console.log("ERROR : ", e.responseText);
          //  $("#btn-search").prop("disabled", false);

        }
    });

	
}

function getquerytext(){
	var query = $('#queryoption').find(":selected").text();
	 $("#issue").val(query);
	// $("#issuevalue").val($('#queryoption').find(":selected").val());
	
}
function fire_ajax_submit() {
	
	
	var search = {}
    search["issue"] = $("#issuevalue").val();
	console.log(search)
	
	$.ajax({
        type: "POST",
        contentType: "application/json",
        url: "/answer",
        data: JSON.stringify(search),
        dataType: 'json',
        cache: false,
        timeout: 600000,
        success: function (data) {

  //          var json = "<h4>Ajax Response</h4><pre>"
  //              + JSON.stringify(data, null, 4) + "</pre>";
  //          $('#feedback').html(json);

        	$('#feedback').html('');
        	$('#issuesuggester').val('');

        	var json="<textarea rows='10' cols='100' >')"
        	 + JSON.stringify(data, null, 4) +"</textarea>";

           

          //  $("#issuesuggester").append(combo);
        	$('#feedback2').html(json);
            
            console.log("SUCCESS : ", data);
            $("#btn-search").prop("disabled", false);

        },
        error: function (e) {
        

        	//$("#issuesuggester").val(e.responseText );

            console.log("ERROR : ", e);
            $("#btn-search").prop("disabled", false);

        }
    });

	
}

function getbotframe1(){
	$.ajax({
        type: "GET",
        contentType: "application/json",
        url: "https://webchat.botframework.com/api/tokens",
        beforeSend: function (xhr){ 
            xhr.setRequestHeader('Authorization', "BotConnector "+ "nz3xM71mRMo.cwA.ecQ.N7qKZyMrNcm2d26xu4bW3ghD6eJ5q0RGyI7mS9LdPis"); 
        },
        cache: false,
        timeout: 600000,
        success: function (data) {

 
        	console.log(data);
        	$('#botframe').html('');
        
        	var tjson="https://webchat.botframework.com/embed/datalakebot?t="
           	 + data;
        	console.log(tjson);
        	var json="<iframe src="+tjson+" ></iframe>";

           console.log(json);

          //  $("#issuesuggester").append(combo);
        	$('#botframe').html(json);
            
            console.log("SUCCESS : ", data);
           

        },
        error: function (e) {
        

        	//$("#issuesuggester").val(e.responseText );

            console.log("ERROR : ", e);
            $("#btn-search").prop("disabled", false);

        }
    });
}


