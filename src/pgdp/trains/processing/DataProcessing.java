package pgdp.trains.processing;

import pgdp.trains.connections.Station;
import pgdp.trains.connections.TrainConnection;
import pgdp.trains.connections.TrainStop;

import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class DataProcessing {

    public static Stream<TrainConnection> cleanDataset(Stream<TrainConnection> connections) {
        // TODO Task 1.
        return connections
                .distinct()
                .sorted(Comparator.comparing(tc -> tc.getFirstStop().scheduled()))
                .map(tc -> tc.withUpdatedStops(
                        tc.stops().stream()
                                .filter(ts -> !ts.kind().equals(TrainStop.Kind.CANCELLED))
                                .collect(Collectors.toList())
                ));
    }

    public static TrainConnection worstDelayedTrain(Stream<TrainConnection> connections) {
        // TODO Task 2.
        //sortiere tc nach ihren maximalen delays
        List<TrainConnection> wd_tc = connections
                .sorted(Comparator.comparingInt(tc -> tc.stops().stream()
                        .mapToInt(ts -> ts.getDelay())
                        .max()
                        .orElse(0)))
                .collect(Collectors.toList());

        return wd_tc.get(wd_tc.size() - 1);
        /*List<TrainConnection> saveConnections = connections
                .collect(Collectors.toList());
        try {
            List<Integer> sortedConnectionsForDelay =
                    saveConnections.stream()
                            .map(tc -> tc.stops().stream()
                                    .mapToInt(ts -> ts.getDelay())
                                    .max())
                            .map(dmax -> dmax.getAsInt())
                            .sorted(Comparator.comparingInt(n -> n))
                            .collect(Collectors.toList());

            if (sortedConnectionsForDelay.get(sortedConnectionsForDelay.size() - 1) == 0) {
                return saveConnections.get(0);
            }

            List<TrainConnection> wd_tc = saveConnections.stream()
                    .filter(tc -> (tc.stops().stream()
                            .mapToInt(ts -> ts.getDelay())
                            .max().getAsInt()) == sortedConnectionsForDelay.get(sortedConnectionsForDelay.size() - 1))
                    .collect(Collectors.toList());
            //System.out.println(wd_tc.get(0).stops().stream().mapToInt(tsList -> tsList.getDelay()).max());
            //System.out.println(sortedConnectionsForDelay);
            return wd_tc.get(0);
        } catch (Exception e) {
            return null;
        }*/
    }

    public static double percentOfKindStops(Stream<TrainConnection> connections, TrainStop.Kind kind) {
        // TODO Task 3.
        double output = connections
                .flatMap(tc -> tc.stops().stream())
                .mapToInt(ts -> ts.kind().equals(kind) ? 1 : 0)
                .average()
                .orElse(0.0);
        return output * 100;
    }

    public static double averageDelayAt(Stream<TrainConnection> connections, Station station) {
        // TODO Task 4.
        double output = connections
                .flatMap(tc -> tc.stops().stream())
                .filter(ts -> ts.station().equals(station))
                .mapToInt(ts -> ts.getDelay())
                .average()
                .orElse(0.0);
        return output;
    }

    public static Map<String, Double> delayComparedToTotalTravelTimeByTransport(Stream<TrainConnection> connections) {
        // TODO Task 5.
        Map<String, Double> output;
        output = connections
                .collect(Collectors.groupingBy(tc -> tc.type()))
                .entrySet().stream()
                .collect(Collectors.toMap(eS -> eS.getKey(), eS -> (eS.getValue().stream()
                        .mapToDouble(tc -> (tc.totalTimeTraveledActual()))
                        .sum() - eS.getValue().stream()
                        .mapToDouble(tc -> (tc.totalTimeTraveledScheduled()))
                        .sum()) / eS.getValue().stream()
                        .mapToDouble(tc -> (tc.totalTimeTraveledActual()))
                        .sum() * 100));
        //handle NaN
        output = output.entrySet().stream()
                .map(entry -> {
                    if (entry.getValue().isNaN()) {
                        entry.setValue(0.0);
                    }
                    return entry;
                })
                .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));

        return output;
        /*List<TrainConnection> saveConnections = connections.collect(Collectors.toList());
        List<String> types = saveConnections.stream()
                .map(tc -> tc.type())
                .distinct()
                .collect(Collectors.toList());
        List<Double> totalactual = types.stream()
                        .map(type -> saveConnections.stream()
                                .mapToDouble(tc -> tc.type().equals(type)
                                        ? tc.totalTimeTraveledActual() : 0)
                                .sum())
                        .collect(Collectors.toList());
        List<Double> totalscheduled = types.stream()
                        .map(type -> saveConnections.stream()
                                .mapToDouble(tc -> tc.type().equals(type)
                                        ? tc.totalTimeTraveledScheduled() : 0)
                                .sum())
                        .collect(Collectors.toList());
        IntStream.range(0, types.size())
                .mapToObj(n -> output.put(types.get(n),
                        (totalactual.get(n) - totalscheduled.get(n)) / totalactual.get(n) * 100))
                .collect(Collectors.toList()); //damit output.put terminiert*/
    }

    public static Map<Integer, Double> averageDelayByHour(Stream<TrainConnection> connections) {
        // TODO Task 6.
        Map<?, ?> output;
        //mit groupingBy nach Trainstop.actual() gruppieren
        output = connections
                .flatMap(tc -> tc.stops().stream())
                .collect(Collectors.groupingBy(ts -> ts.actual().getHour()))
                .entrySet().stream()
                .collect(Collectors.toMap(eS -> eS.getKey(), eS -> eS.getValue().stream()
                        .mapToDouble(ts -> ts.getDelay())
                        .average()
                        .orElse(0.0)));

        return (Map<Integer, Double>) output;
    }

    public static void main(String[] args) {
        // Um alle Verbindungen aus einer Datei zu lesen, verwendet DataAccess.loadFile("path/to/file.json"), etwa:
        // Stream<TrainConnection> trainConnections = DataAccess.loadFile("connections_test/fullDay.json");

        // Oder alternativ über die API, dies aber bitte sparsam verwenden, um die API nicht zu überlasten.
        //Stream<TrainConnection> trainsMunich = DataAccess.getDepartureBoardNowFor(Station.MUENCHEN_HBF);

        List<TrainConnection> trainConnections = List.of(
                new TrainConnection("ICE 2", "ICE", "2", "DB", List.of(
                        new TrainStop(Station.MUENCHEN_HBF,
                                LocalDateTime.of(2022, 12, 1, 11, 0),
                                LocalDateTime.of(2022, 12, 1, 11, 0),
                                TrainStop.Kind.REGULAR),
                        new TrainStop(Station.NUERNBERG_HBF,
                                LocalDateTime.of(2022, 12, 1, 11, 30),
                                LocalDateTime.of(2022, 12, 1, 12, 0),
                                TrainStop.Kind.REGULAR)
                )),
                new TrainConnection("ICE 1", "ICE", "1", "DB", List.of(
                        new TrainStop(Station.MUENCHEN_HBF,
                                LocalDateTime.of(2022, 12, 1, 10, 0),
                                LocalDateTime.of(2022, 12, 1, 10, 0),
                                TrainStop.Kind.REGULAR),
                        new TrainStop(Station.NUERNBERG_HBF,
                                LocalDateTime.of(2022, 12, 1, 10, 30),
                                LocalDateTime.of(2022, 12, 1, 10, 30),
                                TrainStop.Kind.REGULAR)
                )),
                new TrainConnection("ICE 3", "ICE", "3", "DB", List.of(
                        new TrainStop(Station.MUENCHEN_HBF,
                                LocalDateTime.of(2022, 12, 1, 12, 0),
                                LocalDateTime.of(2022, 12, 1, 12, 0),
                                TrainStop.Kind.REGULAR),
                        new TrainStop(Station.AUGSBURG_HBF,
                                LocalDateTime.of(2022, 12, 1, 12, 20),
                                LocalDateTime.of(2022, 12, 1, 13, 0),
                                TrainStop.Kind.CANCELLED),
                        new TrainStop(Station.NUERNBERG_HBF,
                                LocalDateTime.of(2022, 12, 1, 13, 30),
                                LocalDateTime.of(2022, 12, 1, 13, 30),
                                TrainStop.Kind.REGULAR)
                ))
        );

        List<TrainConnection> cleanDataset = cleanDataset(trainConnections.stream()).toList();
        // cleanDataset sollte sortiert sein: [ICE 1, ICE 2, ICE 3] und bei ICE 3 sollte der Stopp in AUGSBURG_HBF
        // nicht mehr enthalten sein.

        TrainConnection worstDelayedTrain = worstDelayedTrain(trainConnections.stream());
        System.out.println(worstDelayedTrain);
        // worstDelayedTrain sollte ICE 3 sein. (Da der Stop in AUGSBURG_HBF mit 40 Minuten Verspätung am spätesten ist.)

        double percentOfKindStops = percentOfKindStops(trainConnections.stream(), TrainStop.Kind.CANCELLED);
        //System.out.println(percentOfKindStops);
        // percentOfKindStops REGULAR sollte 85.71428571428571 sein, CANCELLED 14.285714285714285.

        double averageDelayAt = averageDelayAt(trainConnections.stream(), Station.NUERNBERG_HBF);
        //System.out.println(averageDelayAt);
        // averageDelayAt sollte 10.0 sein. (Da dreimal angefahren und einmal 30 Minuten Verspätung).

        Map<String, Double> delayCompared = delayComparedToTotalTravelTimeByTransport(trainConnections.stream());
        //System.out.println(delayCompared);
        // delayCompared sollte ein Map sein, die für ICE den Wert 16.666666666666668 hat.
        // Da ICE 2 0:30 geplant hatte, aber 1:00 gebraucht hat, ICE 1 0:30 geplant und gebraucht hatte, und
        // ICE 3 1:30 geplant und gebraucht hat. Zusammen also 2:30 geplant und 3:00 gebraucht, und damit
        // (3:00 - 2:30) / 3:00 = 16.666666666666668.

        Map<Integer, Double> averageDelayByHourOfDay = averageDelayByHour(trainConnections.stream());
        System.out.println(averageDelayByHourOfDay);
        // averageDelayByHourOfDay sollte ein Map sein, die für 10, 11 den Wert 0.0 hat, für 12 den Wert 15.0 und
        // für 13 den Wert 20.0.

    }


}
