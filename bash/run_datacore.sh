while true;
do
	${XL_IDP_ROOT_DATACORE}/bash/forecast.sh;
	${XL_IDP_ROOT_DATACORE}/bash/margin_income_plan.sh;
	${XL_IDP_ROOT_DATACORE}/bash/volumes_orlovka_terminal.sh;
	${XL_IDP_ROOT_DATACORE}/bash/border_crossing_plans.sh;
	${XL_IDP_ROOT_DATACORE}/bash/terminals_plans_orlovka_manp.sh;
	${XL_IDP_ROOT_DATACORE}/bash/terminals_plans_p1-p4.sh;
	${XL_IDP_ROOT_DATACORE}/bash/sales_plan_pivot_table.sh;
	sleep 1;
done