package io.github.jimzucker.flinktraining.job;

import io.github.jimzucker.flinktraining.model.Allocation;
import io.github.jimzucker.flinktraining.model.BlockTrade;
import io.github.jimzucker.flinktraining.model.Json;
import io.github.jimzucker.flinktraining.model.Side;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PayloadCarryTest {

    private static Map<String, String> filler(int n) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 1; i <= n; i++) {
            m.put(String.format("f%02d", i), "XXXX");
        }
        return m;
    }

    private static BlockTrade trade(Map<String, String> filler) {
        List<Allocation> allocations = List.of(
                new Allocation("ACC1", "SUB1", 100, filler),
                new Allocation("ACC2", "SUB1", 100, filler),
                new Allocation("ACC3", "SUB1", 100, filler),
                new Allocation("ACC4", "SUB1", 100, filler));
        return new BlockTrade("T1", "AAPL", Side.BUY, 400, allocations, 1_000L);
    }

    private static List<PositionUpdate> run(org.apache.flink.api.common.functions.FlatMapFunction<String, PositionUpdate> f,
                                            BlockTrade t) throws Exception {
        List<PositionUpdate> out = new ArrayList<>();
        f.flatMap(Json.toJson(t), new Collector<>() {
            @Override
            public void collect(PositionUpdate record) {
                out.add(record);
            }

            @Override
            public void close() {
            }
        });
        return out;
    }

    @Test
    @DisplayName("each account-side update carries its allocation's filler, name then value")
    void accountSideCarriesFiller() throws Exception {
        List<PositionUpdate> updates = run(new SplitByAllocation(), trade(filler(32)));

        assertThat(updates).hasSize(4);
        for (PositionUpdate u : updates) {
            assertThat(u.filler).hasSize(64);
            assertThat(u.filler[0]).isEqualTo("f01");
            assertThat(u.filler[1]).isEqualTo("XXXX");
            assertThat(u.filler[62]).isEqualTo("f32");
            assertThat(u.signedQuantity).isEqualTo(100L);
        }
    }

    @Test
    @DisplayName("the symbol side and orders without filler carry nothing extra")
    void nothingExtraWithoutFiller() throws Exception {
        assertThat(run(new ToSymbolUpdate(), trade(filler(32)))).allSatisfy(u -> assertThat(u.filler).isNull());
        assertThat(run(new SplitByAllocation(), trade(Map.of()))).allSatisfy(u -> assertThat(u.filler).isNull());
    }

    @Test
    @DisplayName("PositionUpdate stays a Flink POJO and the filler uses the native string-array serializer, not Kryo")
    void fillerDoesNotFallBackToKryo() {
        TypeInformation<PositionUpdate> info = TypeInformation.of(PositionUpdate.class);
        assertThat(info).isInstanceOf(PojoTypeInfo.class);
        PojoTypeInfo<PositionUpdate> pojo = (PojoTypeInfo<PositionUpdate>) info;
        assertThat(pojo.getTypeAt("filler")).isEqualTo(BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO);
    }
}
