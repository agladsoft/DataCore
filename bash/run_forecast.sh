while true;
do
	${XL_IDP_ROOT_DATACORE}/bash/forecast.sh;
	${XL_IDP_ROOT_DATACORE}/bash/margin_income_plan.sh;
	sleep 1;
done