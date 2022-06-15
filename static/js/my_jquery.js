
$(document).ready(function(){
    $("#filter_table").on("keyup", function() {
        var value = $(this).val().toLowerCase();
        $("table tr").filter(function() {
            $(this).toggle($(this).text().toLowerCase().indexOf(value) > -1)
        });
    });

});

