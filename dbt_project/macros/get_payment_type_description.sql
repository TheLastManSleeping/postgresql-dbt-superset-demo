-- macros/get_payment_type_description.sql

{% macro get_payment_type_description(payment_type_column) %}

    case {{ payment_type_column }}
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        else 'Unknown'
    end

{% endmacro %}