$(function() {

    if(sessionStorage.getItem('dp_start') != null){
        var dp_start = new Date(sessionStorage.getItem('dp_start'));
        var dp_end = new Date(sessionStorage.getItem('dp_end'));
    }
    else {
        var dp_start = moment().subtract(12, 'hours');
        var dp_end = moment();
    }

    
    $('input[name="daterange"]').daterangepicker(
        {
            locale: {"format": "DD/MM/YYYY hh:mm"},
            timePicker: true,
            timePicker24Hour: true,
            startDate: dp_start,
            endDate: dp_end,            
        },
        function(dp_start, dp_end) {
            $.get(
                endpoint,
                {
                    'start': dp_start.toISOString(),
                    'end': dp_end.toISOString(),
                    'thing': thing
                },
                function(dp_data){
                    $("#main_block").html(dp_data)
                }
            );
            sessionStorage.setItem('dp_start', dp_start);
            sessionStorage.setItem('dp_end', dp_end);
        }
        
    );
});
