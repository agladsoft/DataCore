while true;
do
	${XL_IDP_ROOT_DATACORE}/bash/forecast.sh;
	${XL_IDP_ROOT_DATACORE}/bash/margin_income_plan.sh;
	${XL_IDP_ROOT_DATACORE}/bash/volumes_orlovka_terminal.sh;
	${XL_IDP_ROOT_DATACORE}/bash/border_crossing_plans.sh;
	sleep 1;
done