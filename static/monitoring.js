var image_refresh_interval = 60000;

setInterval(function(){
	$("img.refresh").each(function (){
		var jqt = $(this);
		var src = jqt.attr("src").split("?")[0];
		src += "?_ts=" + new Date().getTime();
		jqt.attr("src", src);
	});
}, image_refresh_interval);
