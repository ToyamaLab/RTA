$("#registerForm").validate({
    errorClass:'text-danger',
    errorElement:'span',
    highlight: function (element, errorClass, validClass) { 
        $(element).parents("div[class='form-group']").addClass(errorClass); 
    }, 
    unhighlight: function (element, errorClass, validClass) { 
        $(element).parents(".text-danger").removeClass(errorClass); 
    },
    rules: {
        host: {
            required: true
        },
        user_name: {
            required: true
        },
        password: {
            required: true
        },
        db_name: {
            required: true
        },
        table_name: {
            required: true
        }
    },
    messages: {
        host: "Input Host Name or IP Address",
        user_name: "Input User Name",
        password: "Input Password",
        db_name: "Input Database Name",
        table_name: "Input Table Name",
    }
});
//Validation成功時の遷移先
$("input.validate").click(function() {
    if($("#registerForm").valid() == true){
        location.href = "./register_table.php"
    }
    return false;
});


// $("#addInfoForm").validate({
//     errorClass:'text-danger',
//     errorElement:'span',
//     highlight: function (element, errorClass, validClass) { 
//         $(element).parents("div[class='form-group']").addClass(errorClass); 
//     }, 
//     unhighlight: function (element, errorClass, validClass) { 
//         $(element).parents(".text-danger").removeClass(errorClass); 
//     },
//     rules: {
//         "#access_name": {
//             required: true
//         },
//         "tables[][table_description]": {
//             required: true
//         }
//     },
//     messages: {
//         tables[][access_name]: "Input Access Name",
//         description: {
//             required: true
//         }
//     }
// });
// //Validation成功時の遷移先
// $("input.validate").click(function() {
//     if($("#addInfoForm").valid() == true){
//         location.href = "./register_column.php"
//     }
//     return false;
// });



