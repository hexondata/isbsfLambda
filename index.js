import _ from 'lodash';
import { Trip, Route, Stop, ScheduleV2Timetable } from './models/index.js';
import { Convert, Utils } from './helpers/index.js';
import zlib from 'zlib';

import momentTimezone from 'moment-timezone';
import * as geolib from "geolib";
import PolylineUtils from '@mapbox/polyline'
import { saveToS3 } from './buckets/bucket.js';
import LambdaJobQueue from './models/LambdaJobQueue.js';
momentTimezone.tz.setDefault("Asia/Singapore");


export const handler = async (event) => {
    const {
        from,
        to,
        timestamp = null,
        agencyId = null,
        key,
        jobId
    } = event;


    let tripLog = {}
    let routeStops = {}
    let groupedDataReportt
    let shortNameForRoute;
    const returnData = [];
    if (!timestamp) return {
        statusCode: 500,
        body: JSON.stringify({ message: 'please provide timestamp value' })
    };

    if (!agencyId) return {
        statusCode: 500,
        body: JSON.stringify({ message: 'No Agency Found' })
    };

    let transactionF;
    
    try {
        const jobQueue = await LambdaJobQueue.update('IN_PROGRESS', jobId);
        if (!jobQueue) {
            return {
                statusCode: 500,
                body: JSON.stringify({ message: 'Job ID not found' })
            };
        }

        const data = await Trip.getAgencyClaimReportByDate(agencyId, timestamp, { from, to });

        const whereQueryforTimetable = { agency_id: agencyId };

        const timetableData = await ScheduleV2Timetable.findAllDistinct(whereQueryforTimetable);

        const transactionWithStartFiltered = data.filter(
            ({ startedAt, endedAt, scheduledAt }) =>
                (momentTimezone(endedAt).diff(momentTimezone(startedAt), "minutes") >= 10 &&
                    momentTimezone(startedAt).isSameOrAfter(momentTimezone("2022-09-17 00:00:00"))) ||
                momentTimezone(scheduledAt).isSameOrAfter(momentTimezone("2022-09-17 00:00:00"))
        );

        const transactionWithScheduler = transactionWithStartFiltered.map(
            (trx) => {
                if (trx.startedAt && !trx.scheduledAt) {
                    //
                    let start_time = momentTimezone(String(trx.startedAt)).format("HH:mm:ss");
                    //
                    let dayOfTrip = momentTimezone(String(trx.startedAt)).format(
                        "YYYY-MM-DD"
                    );
                    let dayOfTripName = momentTimezone(String(trx.startedAt)).format("dddd");
                    let end_time = momentTimezone(String(trx.endedAt)).format("HH:mm:ss");

                    let goal = momentTimezone(`2022-01-01 ${start_time}`).format("X");
                    let goalEnd = momentTimezone(`2022-01-01 ${end_time}`).format("X");
                    //
                    let timetableDataTemp = timetableData.filter(
                        ({ day, direction_id, route_id }) =>
                            day.toLowerCase() == dayOfTripName.toLowerCase() &&
                            direction_id == trx.obIb &&
                            route_id == trx.routeId
                    );
                    let closestStart = timetableDataTemp.reduce(function (
                        prev,
                        curr
                    ) {
                        let curr_time = momentTimezone(`2022-01-01 ${curr.start_time}`).format(
                            "X"
                        );

                        return Math.abs(curr_time - goal) < Math.abs(prev - goal)
                            ? curr_time
                            : prev;
                    },
                        0);
                    let closestEnd = timetableDataTemp.reduce(function (prev, curr) {
                        //

                        let curr_time = momentTimezone(`2022-01-01 ${curr.end_time}`).format(
                            "X"
                        );

                        return Math.abs(curr_time - goalEnd) < Math.abs(prev - goalEnd)
                            ? curr_time
                            : prev;
                    }, 0);

                    const closestScheduledAt = `${dayOfTrip} ${momentTimezone
                        .unix(closestStart)
                        .format("HH:mm:ss")}`;

                    const closestScheduledEnd = momentTimezone(
                        `${dayOfTrip} ${momentTimezone.unix(closestEnd).format("HH:mm:ss")}`
                    ).format();

                    return {
                        ...trx,
                        scheduledAt: closestScheduledAt,
                        scheduledEndTime: closestScheduledEnd,
                    };
                } else {
                    return { ...trx };
                }
            }
        );
        //
        transactionF = transactionWithScheduler.filter(
            ({ scheduledAt, scheduledEndTime }) =>
                scheduledAt !== null && scheduledEndTime !== null
        );
        //new justNaik complete trip start
        const uniqueRouteIds = [
            ...new Set(transactionF.map((obj) => obj.routeId)),
        ];

        const routeFetch = uniqueRouteIds.map(async (routeId) => {
            const route = await Route.findOne({ routeId, agencyId });
            if (!route) return res.notFound('Route not found');
            const stops = await Stop.findByRoutes(route.id) || [];
            route.stops = stops;
            return route;
        }

        );

        const result = await Promise.all(
            routeFetch.map((p) => p.catch((e) => e))
        );
        const routeResolve = result.filter(
            (result) => !(result instanceof Error)
        );
        // const tripResolve = await Promise.all(tripFetch)
        // add loader
        routeResolve.forEach(async (data) => {
            if (data.stops.length > 0) {
                routeStops = {
                    ...routeStops,
                    [data?.stops[0]?.routeId]: data.stops,
                };
            }
        });
        //

        //new justNaik complete trip end
        const uniqueTripIds = [
            ...new Set(transactionF.map((obj) => obj.tripId)),
        ];


        const getTripLogForBulk = async (tripId) => {
            return new Promise(async (resolve, reject) => {
                try {
                    const dataFrequency = await Utils.readTripLogFile(tripId)
                    const jsonedLog = await Convert.csvToJson(dataFrequency)
                    if (!jsonedLog) resolve([])
                    resolve(jsonedLog)
                } catch (error) {
                    resolve([])
                }

            })

        }


        if (!uniqueTripIds) return res.unAuthorised('Please provide trip ids')
        const tripFetch = uniqueTripIds.map((tripId) =>
            getTripLogForBulk(tripId)
        )

        const results = await Promise.all(tripFetch.map(p => p.catch(e => e)));
        const tripResolve = results.filter(result => result.length > 0 || !(result instanceof Error));

        tripResolve.forEach(async (data) => {
            if (data.length > 0) {
                //

                const tripLogWithApad = data;

                tripLog = {
                    ...tripLog,
                    [tripLogWithApad[0]?.tripId]: tripLogWithApad,
                };

            }
        });



        //imp
        //   setTransaction(transactionF);




        if (!transactionF) return [];
        const transactionFF = transactionF.filter(
            ({ scheduledAt, scheduledEndTime }) =>
                scheduledAt !== null && scheduledEndTime !== null
        );

        const filtered = transactionFF.filter(
            ({
                scheduledAt,
                routeShortName,
                driverName,
                vehicleRegistrationNumber,
                userId,
            }) => {
                let returnVal = true;



                if (from) {
                    // Adjusting the date to UTC+8
                    const adjustedScheduledAt = new Date(new Date(scheduledAt).getTime() + 8 * 60 * 60 * 1000);

                    returnVal = adjustedScheduledAt.valueOf() >= new Date(from).valueOf();
                    if (!returnVal) return false;
                }

                if (to) {
                    // Adjusting the date to UTC+8
                    const adjustedScheduledAt = new Date(new Date(scheduledAt).getTime() + 8 * 60 * 60 * 1000);


                    returnVal = adjustedScheduledAt.valueOf() <= new Date(to).valueOf();
                    if (!returnVal) return false;
                }



                return true;
            }
        );

        const sortedData = _.orderBy(
            filtered,
            [({ scheduledAt }) => new Date(scheduledAt)],
            ["desc"]
        );
        const addedLocalTime = sortedData?.map((d) => {
            d["localDate"] = d?.scheduledAt
                ? momentTimezone(d.scheduledAt).format("DD-MM-YYYY (ddd)")
                : "undefined";
            return d;
        });
        const groupedData = _.groupBy(addedLocalTime, "localDate");
        // setFilteredTripCollection(groupedData);
        groupedDataReportt = _(addedLocalTime)
            .sortBy("routeId")
            .groupBy("routeName")
            .value();
        //imp
        // setUltraFilteredTripCollection(groupedDataReportt);




        const mainData = groupedData || tripCollection;
        if (!mainData) return [];
        Object.entries(mainData).forEach(([localTimeGroup, trxs]) => {
            //
            const accumulativeTrip = {
                datetime_: momentTimezone(trxs[0].scheduledAt).format(
                    "DD-MM-YYYY HH:mm:ss (ddd)"
                ),
                checkoutTime_: momentTimezone(trxs[0].endedAt).format("DD-MM-YYYY HH:mm:ss"),
                uniqueTrip_: new Set(),
                uniqueScheduledTrip_: new Set(),
                totalTripCount_: 0,
                uniqueIbTrip_: new Set(),
                onTimeTripCount_: 0,
                uniqueOnTimeTrip_: new Set(),
                ibTripCount_: 0,
                uniqueObTrip_: new Set(),
                obTripCount_: 0,
                uniqueObIbTrip_: new Set(),
                obIbTripCount_: 0,
                tripCompilanceCount_: 0,
                uniqueDriver_: new Set(),
                totalUniqueDriverCount_: 0,
                uniqueVehicle_: new Set(),
                totalUniqueVehicleCount_: 0,
                uniqueJourney_: new Set(),
                totalTransaction_: 0,
                totalAmount_: 0,
                noOfAdult: 0,
                noOfChild: 0,
                noOfSenior: 0,
                totalChild: 0,
                totalSenior: 0,
                totalAdult: 0,
                noOfOku: 0,
                noOfForeignAdult: 0,
                noOfForeignChild: 0,
                totalRidership_: 0,
                cashTotalAmount_: 0,
                cashTotalRidership_: 0,
                cashlessTotalAmount_: 0,
                cashlessTotalRidership_: 0,
                uniqueIbTripJ_: new Set(),
                ibTripCountJ_: 0,
                uniqueObTripJ_: new Set(),
                obTripCountJ_: 0,
                uniqueObIbTripJ_: new Set(),
                obIbTripCountJ_: 0,
            };
            trxs.map((row, index) => {

                const totalPax =
                    row.noOfAdult +
                    +row.noOfChild +
                    +row.noOfSenior +
                    +row.noOfOku +
                    +row.noOfForeignAdult +
                    +row.noOfForeignChild;
                accumulativeTrip["uniqueDriver_"].add(row.driverName);
                accumulativeTrip["uniqueVehicle_"].add(row.vehicleRegistrationNumber);
                accumulativeTrip["uniqueTrip_"].add(
                    `${momentTimezone(row.scheduledAt).format("HH:mm")}+${row.obIb}+${row.routeId
                    }`
                );

                if (row?.apadPolygon?.length > 0) {
                    const decodedPolyline = PolylineUtils.decode(row.apadPolygon);

                    const arrIb = [];
                    const arrIbBetween = [];
                    if (decodedPolyline.length > 0 && tripLog[row.tripId]?.length > 0) {
                        decodedPolyline?.forEach((poly, index) => {
                            if (index === 0 || index === 5) {
                                for (
                                    let index = 0;
                                    index < tripLog[row.tripId].length;
                                    index++
                                ) {
                                    const isNear = geolib.isPointWithinRadius(
                                        { latitude: poly[0], longitude: poly[1] },
                                        {
                                            latitude: tripLog[row.tripId][index].latitude,
                                            longitude: tripLog[row.tripId][index].longitude,
                                        },
                                        200
                                    );
                                    if (isNear) {
                                        arrIb.push(poly);
                                        break;
                                    }
                                }
                            }
                        });
                        for (let index = 1; index < decodedPolyline.length - 1; index++) {
                            const element = decodedPolyline[index];
                            for (let index = 0; index < tripLog[row.tripId].length; index++) {
                                const isNear = geolib.isPointWithinRadius(
                                    { latitude: element[0], longitude: element[1] },
                                    {
                                        latitude: tripLog[row.tripId][index].latitude,
                                        longitude: tripLog[row.tripId][index].longitude,
                                    },
                                    200
                                );
                                if (isNear) {
                                    arrIbBetween.push(element);
                                    break;
                                }
                            }
                        }
                    }

                    if (arrIb?.length >= 2 && arrIbBetween?.length >= 1) {
                        row.endedAt &&
                            row.obIb == 2 &&
                            accumulativeTrip["uniqueIbTrip_"].add(row.tripId);
                        row.endedAt &&
                            row.obIb == 1 &&
                            accumulativeTrip["uniqueObTrip_"].add(row.tripId);
                        row.endedAt &&
                            row.obIb == 0 &&
                            accumulativeTrip["uniqueObIbTrip_"].add(row.tripId);
                    }
                }

                if (
                    routeStops[row.routeId]?.filter(
                        ({ directionId }) => directionId == row.obIb
                    )?.length > 0
                ) {
                    const decodedPolyline = routeStops[row.routeId]?.filter(
                        ({ directionId }) => directionId == row.obIb
                    );

                    const arrIb = [];
                    if (decodedPolyline.length > 0 && tripLog[row.tripId]?.length > 0) {
                        decodedPolyline?.forEach((poly, index) => {
                            //
                            for (let index = 0; index < tripLog[row.tripId].length; index++) {
                                const isNear = geolib.isPointWithinRadius(
                                    { latitude: poly.latitude, longitude: poly.longitude },
                                    {
                                        latitude: tripLog[row.tripId][index].latitude,
                                        longitude: tripLog[row.tripId][index].longitude,
                                    },
                                    200
                                );
                                //
                                if (isNear) {
                                    // check if inbound and scheduled?
                                    arrIb.push(poly.sequence);
                                    break;
                                }
                            }
                        });
                    }
                    const uniqueStop = [...new Set(arrIb)];
                    row.endedAt &&
                        row.obIb == 2 &&
                        (routeStops[row.routeId]?.filter(
                            ({ directionId }) => directionId == row.obIb
                        )?.length *
                            15) /
                        100 <=
                        uniqueStop?.length &&
                        accumulativeTrip["uniqueIbTripJ_"].add(row.tripId);
                    row.endedAt &&
                        row.obIb == 1 &&
                        (routeStops[row.routeId]?.filter(
                            ({ directionId }) => directionId == row.obIb
                        )?.length *
                            15) /
                        100 <=
                        uniqueStop?.length &&
                        accumulativeTrip["uniqueObTripJ_"].add(row.tripId);
                    row.endedAt &&
                        row.obIb == 0 &&
                        (routeStops[row.routeId]?.filter(
                            ({ directionId }) => directionId == row.obIb
                        )?.length *
                            15) /
                        100 <=
                        uniqueStop?.length &&
                        accumulativeTrip["uniqueObIbTripJ_"].add(row.tripId);
                }
                // for buStops travel end

                // ib check stop
                accumulativeTrip["uniqueJourney_"].add(row.journeyId);
                accumulativeTrip["totalAmount_"] += +row.amount;
                accumulativeTrip["noOfAdult"] += +row.noOfAdult;
                accumulativeTrip["noOfChild"] += +row.noOfChild;
                accumulativeTrip["noOfSenior"] += +row.noOfSenior;
                accumulativeTrip["noOfOku"] += +row.noOfOku;
                accumulativeTrip["noOfForeignAdult"] += +row.noOfForeignAdult;
                accumulativeTrip["noOfForeignChild"] += +row.noOfForeignChild;
                accumulativeTrip["totalRidership_"] += totalPax;

                accumulativeTrip["cashTotalAmount_"] += row.userId ? 0 : +row.amount;
                accumulativeTrip["cashTotalRidership_"] += row.userId ? 0 : totalPax;

                accumulativeTrip["cashlessTotalAmount_"] += row.userId
                    ? +row.amount
                    : 0;
                accumulativeTrip["cashlessTotalRidership_"] += row.userId
                    ? totalPax
                    : 0;
                if (
                    row.scheduledAt &&
                    row.startedAt &&
                    row.scheduledEndTime &&
                    row.endedAt &&
                    Math.abs(
                        Math.abs(Number(momentTimezone(row.scheduledAt).format("X"))) -
                        Math.abs(Number(momentTimezone(row.startedAt).format("X")))
                    ) <= 300 &&
                    Math.abs(
                        Math.abs(Number(momentTimezone(row.scheduledEndTime).format("X"))) -
                        Math.abs(Number(momentTimezone(row.endedAt).format("X")))
                    ) <= 300
                ) {
                    accumulativeTrip["uniqueOnTimeTrip_"].add(row.tripId);
                } else {
                }
            });

            accumulativeTrip["totalUniqueDriverCount_"] =
                accumulativeTrip.uniqueDriver_.size;
            accumulativeTrip["totalUniqueVehicleCount_"] =
                accumulativeTrip.uniqueVehicle_.size;
            accumulativeTrip["totalTripCount_"] = accumulativeTrip.uniqueTrip_.size;
            accumulativeTrip["totalTransaction_"] =
                accumulativeTrip.uniqueJourney_.size;
            accumulativeTrip["totalAdult"] = accumulativeTrip.noOfAdult;
            accumulativeTrip["ibTripCount_"] = accumulativeTrip.uniqueIbTrip_.size;
            accumulativeTrip["obTripCount_"] = accumulativeTrip.uniqueObTrip_.size;
            accumulativeTrip["obIbTripCount_"] =
                accumulativeTrip.uniqueObIbTrip_.size;
            accumulativeTrip["ibTripCountJ_"] = accumulativeTrip.uniqueIbTripJ_.size;
            accumulativeTrip["obTripCountJ_"] = accumulativeTrip.uniqueObTripJ_.size;
            accumulativeTrip["obIbTripCountJ_"] =
                accumulativeTrip.uniqueObIbTripJ_.size;
            accumulativeTrip["completeTripCount_"] =
                accumulativeTrip.uniqueIbTrip_.size +
                accumulativeTrip.uniqueObTrip_.size +
                accumulativeTrip.uniqueObIbTrip_.size;
            accumulativeTrip["tripCompilanceCount_"] = parseFloat(
                (accumulativeTrip["completeTripCount_"] /
                    accumulativeTrip["totalTripCount_"]) *
                100
            ).toFixed(2);
            accumulativeTrip["localTimeGroup_"] =
                String(localTimeGroup).split("+")[0];
            accumulativeTrip["trxs"] = trxs;
            accumulativeTrip["tripBreakdown"] = "0";
            accumulativeTrip["tripBreakdownP"] =
                accumulativeTrip["completeTripCount_"] > 0 ? "100" : "0";
            accumulativeTrip["offRouteCount_"] =
                accumulativeTrip.uniqueTrip_.size -
                    accumulativeTrip.uniqueIbTrip_.size -
                    accumulativeTrip.uniqueObTrip_.size -
                    accumulativeTrip.uniqueObIbTrip_.size <
                    0
                    ? 0
                    : accumulativeTrip.uniqueTrip_.size -
                    accumulativeTrip.uniqueIbTrip_.size -
                    accumulativeTrip.uniqueObTrip_.size -
                    accumulativeTrip.uniqueObIbTrip_.size;
            accumulativeTrip["routeCompilanceCount_"] =
                accumulativeTrip.uniqueTrip_.size > 0 ? "100%" : "0%";
            accumulativeTrip["kmob_"] = trxs[0].kmOutbound;
            accumulativeTrip["kmib_"] = trxs[0].kmInbound;
            accumulativeTrip["kmloop_"] = trxs[0].kmLoop;
            accumulativeTrip["tkmob_"] =
                accumulativeTrip.uniqueObTrip_.size > 0
                    ? parseFloat(
                        trxs[0].kmOutbound * accumulativeTrip.uniqueObTrip_.size
                    ).toFixed(2)
                    : "";
            accumulativeTrip["tkmib_"] =
                accumulativeTrip.uniqueIbTrip_.size > 0
                    ? parseFloat(
                        trxs[0].kmInbound * accumulativeTrip.uniqueIbTrip_.size
                    ).toFixed(2)
                    : "";
            accumulativeTrip["tkmloop_"] =
                accumulativeTrip.uniqueObIbTrip_.size > 0
                    ? parseFloat(
                        trxs[0].kmLoop * accumulativeTrip.uniqueObIbTrip_.size
                    ).toFixed(2)
                    : "";

            accumulativeTrip["tkm_"] = parseFloat(
                Number(accumulativeTrip["tkmob_"]) +
                Number(accumulativeTrip["tkmib_"]) +
                Number(accumulativeTrip["tkmloop_"])
            ).toFixed(2);

            accumulativeTrip["punctuality"] = accumulativeTrip.uniqueOnTimeTrip_.size;
            accumulativeTrip["punctualityP"] = parseFloat(
                (accumulativeTrip.uniqueOnTimeTrip_.size /
                    accumulativeTrip["totalTripCount_"]) *
                100
            ).toFixed(2);

            //format amount
            accumulativeTrip["totalAmount_"] =
                accumulativeTrip["totalAmount_"].toFixed(2);
            accumulativeTrip["cashTotalAmount_"] =
                accumulativeTrip["cashTotalAmount_"].toFixed(2);
            accumulativeTrip["cashlessTotalAmount_"] =
                accumulativeTrip["cashlessTotalAmount_"].toFixed(2);

            returnData.push(accumulativeTrip);
        });
        //
        //   return res.ok(returnData);

    } catch (error) {
        return {
            statusCode: 500,
            body: JSON.stringify({ message: 'Internal Server Error', error: error.message })
        };
    } finally {
    }



    // EXPORT LOGIC
    const exportData = []
    const mainData2 = groupedDataReportt;
    let strartFrom = momentTimezone(Object.values(mainData2)[0][0].scheduledAt)
        .startOf("month")
        .format("DD-MM-YYYY");
    let endTo = momentTimezone(Object.values(mainData2)[0][0].scheduledAt)
        .endOf("month")
        .format("DD-MM-YYYY");


    if (!mainData2) return [];
    //
    Object.entries(mainData2).forEach(([routeName, trxs], index) => {
        let insideExportData = {}
        const sortedData = _.orderBy(
            trxs,
            [({ scheduledAt }) => new Date(scheduledAt)],
            ["desc"]
        );
        const addedLocalTime = sortedData?.map((d) => {
            d["localDate"] = d?.scheduledAt
                ? momentTimezone(d.scheduledAt).format("DD-MM-YYYY (ddd)")
                : "undefined";
            return d;
        });
        const groupedData = _.groupBy(addedLocalTime, "localDate");
        const currYear = momentTimezone(
            Object.keys(groupedData)[0],
            "DD-MM-YYYY (ddd)"
        ).format("YYYY");
        const currMonth = Object.keys(groupedData)[0].split("-")[1];
        const currMonthName = momentTimezone()
            .month(currMonth - 1)
            .format("MMMM");
        const dateForm = Object.keys(groupedData)[0].split(" ")[0];
        const noOfDays = momentTimezone(dateForm, "DD-MM-YYYY").daysInMonth();
        const dateArr = Array(Number(noOfDays) + 1).fill(0);
        const totalTripArr = Array(Number(noOfDays) + 1).fill(
            0,
            1,
            Number(noOfDays) + 1
        );
        const ibTripArr = Array(Number(noOfDays) + 1).fill(
            0,
            1,
            Number(noOfDays) + 1
        );
        const obTripArr = Array(Number(noOfDays) + 1).fill(
            0,
            1,
            Number(noOfDays) + 1
        );
        const obibTripArr = Array(Number(noOfDays) + 1).fill(
            0,
            1,
            Number(noOfDays) + 1
        );
        const completedTripArr = Array(Number(noOfDays) + 1).fill(
            0,
            1,
            Number(noOfDays) + 1
        );
        const tripComplianceArr = Array(Number(noOfDays) + 1).fill(
            0,
            1,
            Number(noOfDays) + 1
        );
        const ontimeTripArr = Array(Number(noOfDays) + 1).fill(
            0,
            1,
            Number(noOfDays) + 1
        );
        const punctualPercentArr = Array(Number(noOfDays) + 1).fill(
            0,
            1,
            Number(noOfDays) + 1
        );
        const vehicleArr = Array(Number(noOfDays) + 1).fill(
            0,
            1,
            Number(noOfDays) + 1
        );
        const fareboxArr = Array(Number(noOfDays) + 1).fill(
            0,
            1,
            Number(noOfDays) + 1
        );
        const ridershipArr = Array(Number(noOfDays) + 1).fill(
            0,
            1,
            Number(noOfDays) + 1
        );
        const tripBreakdownArr = Array(Number(noOfDays) + 1).fill(
            0,
            1,
            Number(noOfDays) + 1
        );
        const tripBreakdownPArr = Array(Number(noOfDays) + 1).fill(
            0,
            1,
            Number(noOfDays) + 1
        );
        const offRouteCount_Arr = Array(Number(noOfDays) + 1).fill(
            0,
            1,
            Number(noOfDays) + 1
        );
        const routeCompilanceCount_Arr = Array(Number(noOfDays) + 1).fill(
            0,
            1,
            Number(noOfDays) + 1
        );
        const kmob_Arr = Array(Number(noOfDays) + 1).fill(
            0,
            1,
            Number(noOfDays) + 1
        );
        const kmib_Arr = Array(Number(noOfDays) + 1).fill(
            0,
            1,
            Number(noOfDays) + 1
        );
        const kmloop_Arr = Array(Number(noOfDays) + 1).fill(
            0,
            1,
            Number(noOfDays) + 1
        );
        const tkmob_Arr = Array(Number(noOfDays) + 1).fill(
            0,
            1,
            Number(noOfDays) + 1
        );
        const tkmib_Arr = Array(Number(noOfDays) + 1).fill(
            0,
            1,
            Number(noOfDays) + 1
        );
        const tkmloop_Arr = Array(Number(noOfDays) + 1).fill(
            0,
            1,
            Number(noOfDays) + 1
        );
        const tkm_Arr = Array(Number(noOfDays) + 1).fill(
            0,
            1,
            Number(noOfDays) + 1
        );
        const ibTripJArr = Array(Number(noOfDays) + 1).fill(
            0,
            1,
            Number(noOfDays) + 1
        );
        const obTripJArr = Array(Number(noOfDays) + 1).fill(
            0,
            1,
            Number(noOfDays) + 1
        );
        const obibTripJArr = Array(Number(noOfDays) + 1).fill(
            0,
            1,
            Number(noOfDays) + 1
        );
        Object.entries(groupedData).forEach(([scheduledAt, trxs]) => {

            const accumulativeTrip = {
                datetime_: momentTimezone(trxs[0].scheduledAt).format(
                    "DD-MM-YYYY HH:mm:ss (ddd)"
                ),
                checkoutTime_: momentTimezone(trxs[0].endedAt).format("DD-MM-YYYY HH:mm:ss"),
                uniqueTrip_: new Set(),
                uniqueScheduledTrip_: new Set(),
                totalTripCount_: 0,
                uniqueIbTrip_: new Set(),
                onTimeTripCount_: 0,
                uniqueOnTimeTrip_: new Set(),
                ibTripCount_: 0,
                uniqueObTrip_: new Set(),
                obTripCount_: 0,
                uniqueObIbTrip_: new Set(),
                obIbTripCount_: 0,
                tripCompilanceCount_: 0,
                uniqueDriver_: new Set(),
                totalUniqueDriverCount_: 0,
                uniqueVehicle_: new Set(),
                totalUniqueVehicleCount_: 0,
                uniqueJourney_: new Set(),
                totalTransaction_: 0,
                totalAmount_: 0,
                noOfAdult: 0,
                noOfChild: 0,
                noOfSenior: 0,
                totalChild: 0,
                totalSenior: 0,
                totalAdult: 0,
                noOfOku: 0,
                noOfForeignAdult: 0,
                noOfForeignChild: 0,
                totalRidership_: 0,
                cashTotalAmount_: 0,
                cashTotalRidership_: 0,
                cashlessTotalAmount_: 0,
                cashlessTotalRidership_: 0,
                uniqueIbTripJ_: new Set(),
                ibTripCountJ_: 0,
                uniqueObTripJ_: new Set(),
                obTripCountJ_: 0,
                uniqueObIbTripJ_: new Set(),
                obIbTripCountJ_: 0,
            };
            trxs.forEach((row) => {
                //
                shortNameForRoute = row.routeShortName;
                const totalPax =
                    row.noOfAdult +
                    +row.noOfChild +
                    +row.noOfSenior +
                    +row.noOfOku +
                    +row.noOfForeignAdult +
                    +row.noOfForeignChild;
                accumulativeTrip["uniqueDriver_"].add(row.driverName);
                accumulativeTrip["uniqueVehicle_"].add(row.vehicleRegistrationNumber);
                accumulativeTrip["uniqueTrip_"].add(
                    `${momentTimezone(row.scheduledAt).format("HH:mm")}+${row.obIb}`
                );

                if (row?.apadPolygon?.length > 0) {
                    const decodedPolyline = PolylineUtils.decode(row.apadPolygon);
                    const arrIb = [];
                    const arrIbBetween = [];
                    if (decodedPolyline.length > 0 && tripLog[row.tripId]?.length > 0) {
                        decodedPolyline?.forEach((poly, index) => {
                            if (index === 0 || index === 5) {
                                for (
                                    let index = 0;
                                    index < tripLog[row.tripId].length;
                                    index++
                                ) {
                                    const isNear = geolib.isPointWithinRadius(
                                        { latitude: poly[0], longitude: poly[1] },
                                        {
                                            latitude: tripLog[row.tripId][index].latitude,
                                            longitude: tripLog[row.tripId][index].longitude,
                                        },
                                        200
                                    );
                                    //
                                    if (isNear) {
                                        // check if inbound and scheduled?
                                        arrIb.push(poly);
                                        break;
                                    }
                                }
                            }
                        });
                        for (let index = 1; index < decodedPolyline.length - 1; index++) {
                            const element = decodedPolyline[index];
                            for (
                                let index = 0;
                                index < tripLog[row.tripId].length;
                                index++
                            ) {
                                const isNear = geolib.isPointWithinRadius(
                                    { latitude: element[0], longitude: element[1] },
                                    {
                                        latitude: tripLog[row.tripId][index].latitude,
                                        longitude: tripLog[row.tripId][index].longitude,
                                    },
                                    200
                                );
                                //
                                if (isNear) {
                                    // check if inbound and scheduled?
                                    arrIbBetween.push(element);
                                    break;
                                }
                            }
                        }
                    }

                    if (arrIb?.length >= 2 && arrIbBetween?.length >= 1) {
                        row.endedAt &&
                            row.obIb == 2 &&
                            accumulativeTrip["uniqueIbTrip_"].add(row.tripId);
                        row.endedAt &&
                            row.obIb == 1 &&
                            accumulativeTrip["uniqueObTrip_"].add(row.tripId);
                        row.endedAt &&
                            row.obIb == 0 &&
                            accumulativeTrip["uniqueObIbTrip_"].add(row.tripId);
                    }
                }

                // for buStops travel start
                if (
                    routeStops[row.routeId]?.filter(
                        ({ directionId }) => directionId == row.obIb
                    )?.length > 0
                ) {
                    const decodedPolyline = routeStops[row.routeId]?.filter(
                        ({ directionId }) => directionId == row.obIb
                    );

                    const arrIb = [];
                    if (decodedPolyline.length > 0 && tripLog[row.tripId]?.length > 0) {
                        decodedPolyline?.forEach((poly, index) => {
                            //
                            for (
                                let index = 0;
                                index < tripLog[row.tripId].length;
                                index++
                            ) {
                                const isNear = geolib.isPointWithinRadius(
                                    { latitude: poly.latitude, longitude: poly.longitude },
                                    {
                                        latitude: tripLog[row.tripId][index].latitude,
                                        longitude: tripLog[row.tripId][index].longitude,
                                    },
                                    200
                                );
                                //
                                if (isNear) {
                                    // check if inbound and scheduled?
                                    arrIb.push(poly.sequence);
                                    break;
                                }
                            }
                        });
                    }
                    const uniqueStop = [...new Set(arrIb)];
                    row.endedAt &&
                        row.obIb == 2 &&
                        (routeStops[row.routeId]?.filter(
                            ({ directionId }) => directionId == row.obIb
                        )?.length *
                            15) /
                        100 <=
                        uniqueStop?.length &&
                        accumulativeTrip["uniqueIbTripJ_"].add(row.tripId);
                    row.endedAt &&
                        row.obIb == 1 &&
                        (routeStops[row.routeId]?.filter(
                            ({ directionId }) => directionId == row.obIb
                        )?.length *
                            15) /
                        100 <=
                        uniqueStop?.length &&
                        accumulativeTrip["uniqueObTripJ_"].add(row.tripId);
                    row.endedAt &&
                        row.obIb == 0 &&
                        (routeStops[row.routeId]?.filter(
                            ({ directionId }) => directionId == row.obIb
                        )?.length *
                            15) /
                        100 <=
                        uniqueStop?.length &&
                        accumulativeTrip["uniqueObIbTripJ_"].add(row.tripId);
                }
                // for buStops travel end
                accumulativeTrip["uniqueJourney_"].add(row.journeyId);
                accumulativeTrip["totalAmount_"] += +row.amount;
                accumulativeTrip["noOfAdult"] += +row.noOfAdult;
                accumulativeTrip["noOfChild"] += +row.noOfChild;
                accumulativeTrip["noOfSenior"] += +row.noOfSenior;
                accumulativeTrip["noOfOku"] += +row.noOfOku;
                accumulativeTrip["noOfForeignAdult"] += +row.noOfForeignAdult;
                accumulativeTrip["noOfForeignChild"] += +row.noOfForeignChild;
                accumulativeTrip["totalRidership_"] += totalPax;

                accumulativeTrip["cashTotalAmount_"] += row.userId ? 0 : +row.amount;
                accumulativeTrip["cashTotalRidership_"] += row.userId ? 0 : totalPax;

                accumulativeTrip["cashlessTotalAmount_"] += row.userId
                    ? +row.amount
                    : 0;
                accumulativeTrip["cashlessTotalRidership_"] += row.userId
                    ? totalPax
                    : 0;
                accumulativeTrip["localTimeGroup_"] = row.scheduledAt
                    ? momentTimezone(row.scheduledAt).format("DD-MM-YYYY (ddd)")
                    : "undefined";
                if (
                    row.scheduledAt &&
                    row.startedAt &&
                    row.scheduledEndTime &&
                    row.endedAt &&
                    Math.abs(
                        Math.abs(Number(momentTimezone(row.scheduledAt).format("X"))) -
                        Math.abs(Number(momentTimezone(row.startedAt).format("X")))
                    ) <= 300 &&
                    Math.abs(
                        Math.abs(Number(momentTimezone(row.scheduledEndTime).format("X"))) -
                        Math.abs(Number(momentTimezone(row.endedAt).format("X")))
                    ) <= 300
                ) {
                    accumulativeTrip["uniqueOnTimeTrip_"].add(row.tripId);
                } else {
                }
            });
            accumulativeTrip["totalUniqueDriverCount_"] =
                accumulativeTrip.uniqueDriver_.size;
            accumulativeTrip["totalUniqueVehicleCount_"] =
                accumulativeTrip.uniqueVehicle_.size;
            accumulativeTrip["totalTripCount_"] = accumulativeTrip.uniqueTrip_.size;
            accumulativeTrip["totalTransaction_"] =
                accumulativeTrip.uniqueJourney_.size;
            accumulativeTrip["totalAdult"] = accumulativeTrip.noOfAdult;
            accumulativeTrip["ibTripCount_"] = accumulativeTrip.uniqueIbTrip_.size;
            accumulativeTrip["obTripCount_"] = accumulativeTrip.uniqueObTrip_.size;
            accumulativeTrip["obIbTripCount_"] =
                accumulativeTrip.uniqueObIbTrip_.size;
            accumulativeTrip["ibTripCountJ_"] =
                accumulativeTrip.uniqueIbTripJ_.size;
            accumulativeTrip["obTripCountJ_"] =
                accumulativeTrip.uniqueObTripJ_.size;
            accumulativeTrip["obIbTripCountJ_"] =
                accumulativeTrip.uniqueObIbTripJ_.size;
            accumulativeTrip["completeTripCount_"] =
                accumulativeTrip.uniqueIbTrip_.size +
                accumulativeTrip.uniqueObTrip_.size +
                accumulativeTrip.uniqueObIbTrip_.size;
            accumulativeTrip["tripCompilanceCount_"] = parseFloat(
                (accumulativeTrip["completeTripCount_"] /
                    accumulativeTrip["totalTripCount_"]) *
                100
            ).toFixed(2);
            accumulativeTrip["trxs"] = trxs;
            accumulativeTrip["date"] = String(
                accumulativeTrip["localTimeGroup_"]
            ).slice(0, 2);
            accumulativeTrip["punctuality"] =
                accumulativeTrip.uniqueOnTimeTrip_.size;
            accumulativeTrip["punctualityP"] = parseFloat(
                (accumulativeTrip.uniqueOnTimeTrip_.size /
                    accumulativeTrip["totalTripCount_"]) *
                100
            ).toFixed(2);

            accumulativeTrip["tripBreakdown"] = "0";
            accumulativeTrip["tripBreakdownP"] =
                accumulativeTrip["completeTripCount_"] > 0 ? "100" : "0";

            accumulativeTrip["offRouteCount_"] =
                accumulativeTrip.uniqueTrip_.size -
                    accumulativeTrip.uniqueIbTrip_.size -
                    accumulativeTrip.uniqueObTrip_.size -
                    accumulativeTrip.uniqueObIbTrip_.size <
                    0
                    ? 0
                    : accumulativeTrip.uniqueTrip_.size -
                    accumulativeTrip.uniqueIbTrip_.size -
                    accumulativeTrip.uniqueObTrip_.size -
                    accumulativeTrip.uniqueObIbTrip_.size;
            accumulativeTrip["routeCompilanceCount_"] =
                accumulativeTrip.uniqueTrip_.size > 0 ? "100" : "0";
            accumulativeTrip["kmob_"] = trxs[0].kmOutbound;
            accumulativeTrip["kmib_"] = trxs[0].kmInbound;
            accumulativeTrip["kmloop_"] = trxs[0].kmLoop;
            accumulativeTrip["tkmob_"] =
                accumulativeTrip.uniqueObTrip_.size > 0
                    ? parseFloat(
                        trxs[0].kmOutbound * accumulativeTrip.uniqueObTrip_.size
                    ).toFixed(2)
                    : "";
            accumulativeTrip["tkmib_"] =
                accumulativeTrip.uniqueIbTrip_.size > 0
                    ? parseFloat(
                        trxs[0].kmInbound * accumulativeTrip.uniqueIbTrip_.size
                    ).toFixed(2)
                    : "";
            accumulativeTrip["tkmloop_"] =
                accumulativeTrip.uniqueObIbTrip_.size > 0
                    ? parseFloat(
                        trxs[0].kmLoop * accumulativeTrip.uniqueObIbTrip_.size
                    ).toFixed(2)
                    : "";

            accumulativeTrip["tkm_"] = parseFloat(
                Number(accumulativeTrip["tkmob_"]) +
                Number(accumulativeTrip["tkmib_"]) +
                Number(accumulativeTrip["tkmloop_"])
            ).toFixed(2);

            //format amount
            accumulativeTrip["totalAmount_"] =
                accumulativeTrip["totalAmount_"].toFixed(2);
            accumulativeTrip["cashTotalAmount_"] =
                accumulativeTrip["cashTotalAmount_"].toFixed(2);
            accumulativeTrip["cashlessTotalAmount_"] =
                accumulativeTrip["cashlessTotalAmount_"].toFixed(2);

            // returnData.push(accumulativeTrip)
            //
            dateArr[Number(accumulativeTrip["date"])] = Number(
                String(accumulativeTrip["localTimeGroup_"]).slice(0, 2)
            );
            totalTripArr[Number(accumulativeTrip["date"])] = Number(
                accumulativeTrip["totalTripCount_"]
            );
            ibTripArr[Number(accumulativeTrip["date"])] = Number(
                accumulativeTrip["ibTripCount_"]
            );
            obTripArr[Number(accumulativeTrip["date"])] = Number(
                accumulativeTrip["obTripCount_"]
            );
            obibTripArr[Number(accumulativeTrip["date"])] = Number(
                accumulativeTrip["obIbTripCount_"]
            );
            ibTripJArr[Number(accumulativeTrip["date"])] = Number(
                accumulativeTrip["ibTripCountJ_"]
            );
            obTripJArr[Number(accumulativeTrip["date"])] = Number(
                accumulativeTrip["obTripCountJ_"]
            );
            obibTripJArr[Number(accumulativeTrip["date"])] = Number(
                accumulativeTrip["obIbTripCountJ_"]
            );
            completedTripArr[Number(accumulativeTrip["date"])] = Number(
                accumulativeTrip["completeTripCount_"]
            );
            tripComplianceArr[Number(accumulativeTrip["date"])] = Number(
                accumulativeTrip["tripCompilanceCount_"]
            );
            ontimeTripArr[Number(accumulativeTrip["date"])] =
                accumulativeTrip["punctuality"];
            punctualPercentArr[Number(accumulativeTrip["date"])] = Number(
                accumulativeTrip["punctualityP"]
            );
            vehicleArr[Number(accumulativeTrip["date"])] = Number(
                accumulativeTrip["totalUniqueVehicleCount_"]
            );
            fareboxArr[Number(accumulativeTrip["date"])] = Number(
                accumulativeTrip["totalAmount_"]
            );
            ridershipArr[Number(accumulativeTrip["date"])] = Number(
                accumulativeTrip["totalRidership_"]
            );
            tripBreakdownPArr[Number(accumulativeTrip["date"])] = Number(
                accumulativeTrip["tripBreakdownP"]
            );

            offRouteCount_Arr[Number(accumulativeTrip["date"])] = Number(
                accumulativeTrip["offRouteCount_"]
            );
            routeCompilanceCount_Arr[Number(accumulativeTrip["date"])] = Number(
                accumulativeTrip["routeCompilanceCount_"]
            );
            kmob_Arr[Number(accumulativeTrip["date"])] = Number(
                accumulativeTrip["kmob_"]
            );
            kmib_Arr[Number(accumulativeTrip["date"])] = Number(
                accumulativeTrip["kmib_"]
            );
            kmloop_Arr[Number(accumulativeTrip["date"])] = Number(
                accumulativeTrip["kmloop_"]
            );
            tkmob_Arr[Number(accumulativeTrip["date"])] = Number(
                accumulativeTrip["tkmob_"]
            );
            tkmib_Arr[Number(accumulativeTrip["date"])] = Number(
                accumulativeTrip["tkmib_"]
            );
            tkmloop_Arr[Number(accumulativeTrip["date"])] = Number(
                accumulativeTrip["tkmloop_"]
            );
            tkm_Arr[Number(accumulativeTrip["date"])] = Number(
                accumulativeTrip["tkm_"]
            );
        });

        insideExportData.shortNameForRoute = shortNameForRoute.toUpperCase()
        insideExportData.routeName = routeName.toUpperCase()
        insideExportData.totalTripArr = totalTripArr
        insideExportData.obTripArr = obTripArr
        insideExportData.ibTripArr = ibTripArr
        insideExportData.completedTripArr = completedTripArr
        insideExportData.tripComplianceArr = tripComplianceArr
        insideExportData.offRouteCount_Arr = offRouteCount_Arr
        insideExportData.routeCompilanceCount_Arr = routeCompilanceCount_Arr
        insideExportData.kmob_Arr = kmob_Arr
        insideExportData.kmib_Arr = kmib_Arr
        insideExportData.totalTripArr = totalTripArr
        insideExportData.tkmob_Arr = tkmob_Arr
        insideExportData.tkmib_Arr = tkmib_Arr
        insideExportData.tkm_Arr = tkm_Arr
        insideExportData.ontimeTripArr = ontimeTripArr
        insideExportData.punctualPercentArr = punctualPercentArr
        insideExportData.tripBreakdownArr = tripBreakdownArr
        insideExportData.tripBreakdownPArr = tripBreakdownPArr
        insideExportData.vehicleArr = vehicleArr
        insideExportData.fareboxArr = fareboxArr
        insideExportData.ridershipArr = ridershipArr
        insideExportData.totalTripArr = totalTripArr

        exportData.push(insideExportData)


    })
    returnData.exportData = exportData

    const jsonResponse = JSON.stringify({ returnData, exportData }).toString('base64');

    // Compress the JSON string
    const compressedData = zlib.gzipSync(jsonResponse);

    try {
        await saveToS3('justnaik-lambda-reports', key, compressedData);
        const jobQueue = await LambdaJobQueue.update('COMPLETED', jobId);
    } catch (error) { 
        const jobQueue = await LambdaJobQueue.update('FAILED', jobId);
        console.error('Error uploading to bucket: ', error);
        return {
            statusCode: 500,
            body: JSON.stringify({ message: 'Internal Server Error', error: error.message })
        };
    }

    return {
        statusCode: 200,
        body: compressedData
    };
}