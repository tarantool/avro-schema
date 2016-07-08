var TNT_URI = '/tarantool';

function request(method, params){
    $.post(
        TNT_URI,
        JSON.stringify(
            {
                "method": method, 
                "params": params, 
                "id": 1
            }
        ),
        function(res){
            var html = '<pre><code class="PYTHON">';
	    html += JSON.stringify(res.result[0], '', 2) + '\n';
	    html == "</code></pre>";
            $('#requests').html(html);
	    $('pre code').each(function(i, block) {
		hljs.highlightBlock(block);
	    });
        }
    );
}

function compile() {
    var schema = $('#schema').val();
    request('compile', [schema]);
}

function validate() {
    var schema = $('#schema').val();
    var data = $('#data').val();
    request('validate', [schema, data]);
}

function set_editor(id){
    var text = document.getElementById(id);
    var editableCodeMirror = CodeMirror.fromTextArea(text, {
        mode: "javascript",
        theme: "default",
        lineNumbers: true,
    });
    editableCodeMirror.on('change', function (cm) {
        text.value = cm.getValue();
    });
}

$(document).ready(function(){
    $(document).on('click', '#btn_compile', compile);
    $(document).on('click', '#btn_validate', validate);
    $.map(['schema', 'data'], function(item){
        set_editor(item);
    });
});
