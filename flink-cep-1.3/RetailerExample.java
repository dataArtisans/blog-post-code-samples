/** This is a code sample written with FlinkCEP 1.3.2. It is not meant to be compiled on its own, but you can follow the steps in the quickstart guide (https://ci.apache.org/projects/flink/flink-docs-release-1.3/quickstart/java_api_quickstart.html) to create a Flink program . */

package org.apache.flink.cep;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class RetailerExample {

	/** The initial source of our shipment. */
	private static final String SRC = "a";

	/** The final destination of our shipment. */
	private static final String DST = "h";

	/** The required intermediate stops. */
	private static final int NUM_STOPS = 5;

	/**
	 * A pattern that searches for shipments that:
	 * 1) start from SRC
	 * 2) finish in DST
	 * 3) have at least NUM_STOPS stops in-between.
	 */
	private static final Pattern<Transport, ?> pattern = Pattern.<Transport>begin("start")
			.where(new SimpleCondition<Transport>() {
				private static final long serialVersionUID = 314415972814127035L;

				@Override
				public boolean filter(Transport value) throws Exception {
					return Objects.equals(value.getFrom(), SRC);
				}
			}).followedBy("middle").where(new IterativeCondition<Transport>() {
				private static final long serialVersionUID = 6664468385615273240L;

				@Override
				public boolean filter(Transport value, Context<Transport> ctx) throws Exception {
					Iterable<Transport> middleStops = ctx.getEventsForPattern("middle");
					String currentLocation = middleStops.iterator().hasNext() ?
							getLastDestinationAndStopCountForPattern(middleStops).f0 : 				// see if we start where the last shipment finished.
							getLastDestinationAndStopCountForPattern(ctx, "start").f0;	// for the first shipment after the package left the SRC
					return Objects.equals(value.getFrom(), currentLocation);
				}
			}).oneOrMore().followedBy("end").where(new IterativeCondition<Transport>() {
				private static final long serialVersionUID = 5721311694340771858L;

				@Override
				public boolean filter(Transport value, Context<Transport> ctx) throws Exception {
					Tuple2<String, Integer> locationAndStopCount =
							getLastDestinationAndStopCountForPattern(ctx,"middle");

					return locationAndStopCount.f1 >= (NUM_STOPS - 1) && 					// at least NUM_STOPS in between SRC and DST
							Objects.equals(locationAndStopCount.f0, value.getFrom()) &&
							Objects.equals(value.getTo(), DST);								// final destination == DST
				}
			}).within(Time.hours(24L));

	public static void main(String[] args) throws Exception {

		List<Transport> sampleData = new ArrayList<>();
		sampleData.add(new Transport(1, "a", "b", 0L));
		sampleData.add(new Transport(1, "b", "c", 1L));
		sampleData.add(new Transport(1, "c", "d", 2L));
		sampleData.add(new Transport(1, "d", "e", 3L));
		sampleData.add(new Transport(1, "e", "f", 4L));
		sampleData.add(new Transport(1, "f", "g", 5L));
		sampleData.add(new Transport(1, "g", "h", 6L));

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStream<Transport> keyedInput = env.fromCollection(sampleData).keyBy(element -> element.getProductId());

		CEP.pattern(keyedInput, pattern).flatSelect(new PatternFlatSelectFunction<Transport, String>() {
			private static final long serialVersionUID = -8972838879934875538L;

			@Override
			public void flatSelect(Map<String, List<Transport>> map, Collector<String> collector) throws Exception {
				StringBuilder str = new StringBuilder();
				for (Map.Entry<String, List<Transport>> entry: map.entrySet()) {
					for (Transport t: entry.getValue()) {
						str.append(t +" ");
					}
				}
				collector.collect(str.toString());
			}
		}).print();

		env.execute();
	}

	/**
	 * Our input records. Each contains:
	 * 1. the id of the product,
	 * 2. the starting location of the shipment, and
	 * 3. the final location of the shipment.
	 */
	public static class Transport {

		private final int prodId;
		private final String from;
		private final String to;

		private final long timestamp;

		public Transport(int productId, String from, String to, long timestamp) {
			this.prodId = productId;
			this.from = from;
			this.to = to;
			this.timestamp = timestamp;
		}

		public int getProductId() {
			return prodId;
		}

		public String getFrom() {
			return from;
		}

		public String getTo() {
			return to;
		}

		public long getTimestamp() {
			return timestamp;
		}

		@Override
		public String toString() {
			return "Transport{" +
					"prodId=" + prodId +
					", from='" + from + '\'' +
					", to='" + to + '\'' +
					'}';
		}
	}

	private static Tuple2<String, Integer> getLastDestinationAndStopCountForPattern(IterativeCondition.Context<Transport> ctx, String patternName) {
		return getLastDestinationAndStopCountForPattern(ctx.getEventsForPattern(patternName));
	}

	private static Tuple2<String, Integer> getLastDestinationAndStopCountForPattern(Iterable<Transport> events) {
		Tuple2<String, Integer> locationAndStopCount = new Tuple2<>("", 0);

		for (Transport transport : events) {
			locationAndStopCount.f0 = transport.getTo();
			locationAndStopCount.f1++;
		}
		return locationAndStopCount;
	}
}
