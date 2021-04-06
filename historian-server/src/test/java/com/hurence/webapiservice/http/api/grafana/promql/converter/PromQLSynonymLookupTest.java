package com.hurence.webapiservice.http.api.grafana.promql.converter;

import com.hurence.webapiservice.http.api.grafana.promql.converter.PromQLSynonymLookup;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.Assert.*;

class PromQLSynonymLookupTest {


    @Test
    void getOriginalAndSynonyms() {

        String original = "DC1_HST1_CPU";
        String synonym = "cpu_usage";

        PromQLSynonymLookup.enable();
        PromQLSynonymLookup.put(original, synonym);
        assertEquals(original, PromQLSynonymLookup.getOriginal(original));
        assertEquals(original, PromQLSynonymLookup.getOriginal(synonym));

        assertEquals(synonym, PromQLSynonymLookup.getSynonym(original));
        assertEquals(synonym, PromQLSynonymLookup.getSynonym(synonym));

        assertEquals("cpu_usage", PromQLSynonymLookup.getSynonymName(original));
        assertEquals("cpu_usage", PromQLSynonymLookup.getSynonymName(synonym));

    }


    @Test
    void getNameFromSynonymWithStrings() {

        String original = "DC1_HST1_CPU";
        String synonym = "cpu_usage{data_center=DC1, host=HST1}";

        PromQLSynonymLookup.enable();
        PromQLSynonymLookup.put(original, synonym);
        assertEquals(original, PromQLSynonymLookup.getOriginal(original));
        assertEquals(original, PromQLSynonymLookup.getOriginal(synonym));

        assertEquals(synonym, PromQLSynonymLookup.getSynonym(original));
        assertEquals(synonym, PromQLSynonymLookup.getSynonym(synonym));

        assertEquals("cpu_usage", PromQLSynonymLookup.getSynonymName(original));
        assertEquals("cpu_usage", PromQLSynonymLookup.getSynonymName(synonym));

    }

    @Test
    void getNameFromSynonymWithStringsAndTags() {

        String original = "DC1_HST1_CPU{data_center=DC1, host=HST1}";
        String synonym = "cpu_usage{data_center=DC1, host=HST1}";

        PromQLSynonymLookup.enable();
        PromQLSynonymLookup.put(original, synonym);
        assertEquals(original, PromQLSynonymLookup.getOriginal(original));
        assertEquals(original, PromQLSynonymLookup.getOriginal(synonym));

        assertEquals(synonym, PromQLSynonymLookup.getSynonym(original));
        assertEquals(synonym, PromQLSynonymLookup.getSynonym(synonym));

        assertEquals("cpu_usage", PromQLSynonymLookup.getSynonymName(original));
        assertEquals("cpu_usage", PromQLSynonymLookup.getSynonymName(synonym));
        assertEquals("cpu_usage", PromQLSynonymLookup.getSynonymName("DC1_HST1_CPU"));

    }

    @Test
    void getTagsBySynonymName() {

        String original = "DC1_HST1_CPU";
        String synonym = "cpu_usage{data_center=DC1, host=HST1}";

        PromQLSynonymLookup.enable();
        PromQLSynonymLookup.put(original, synonym);

        PromQLSynonymLookup.put("cpu_prct_used{metric_id=ac8519bc-2985-4869-8aa8-c5fdfc68ec44}", "cpu_prct_used_renamed{metric_id=ac8519bc-2985-4869-8aa8-c5fdfc68ec44}");
        PromQLSynonymLookup.put("cpu_prct_used{metric_id=ae320972-f612-4cef-8570-908250b8bd1a}", "cpu_prct_used_renamed{metric_id=ae320972-f612-4cef-8570-908250b8bd1a}");
        PromQLSynonymLookup.put("cpu_prct_used{metric_id=af33574c-c852-4b76-9e67-b19ad444250d}", "cpu_prct_used_renamed{metric_id=af33574c-c852-4b76-9e67-b19ad444250d}");
        PromQLSynonymLookup.put("cpu_prct_used{metric_id=b2c2b01e-2c54-4d05-9687-b69cab6cfc68}", "cpu_prct_used_renamed{metric_id=b2c2b01e-2c54-4d05-9687-b69cab6cfc68}");

        assertEquals("data_center", PromQLSynonymLookup.getSwappedTagsBySynonymName("cpu_usage").get("DC1"));
        assertEquals("host", PromQLSynonymLookup.getSwappedTagsBySynonymName("cpu_usage").get("HST1"));
        assertNull(PromQLSynonymLookup.getSwappedTagsBySynonymName("plik"));

        assertEquals("metric_id", PromQLSynonymLookup.getSwappedTagsBySynonymName("cpu_prct_used_renamed").get("ac8519bc-2985-4869-8aa8-c5fdfc68ec44" ));
        assertEquals(4, PromQLSynonymLookup.getSwappedTagsBySynonymName("cpu_prct_used_renamed").values().size());
    }

    @Test
    void getTagsByNameWithDot() {

        String original = "DC1_HST1_CPU.aze{data_center=DC1, host=HST1}";
        String synonym = "cpu_usage{data_center=DC1, host=HST1}";

        PromQLSynonymLookup.enable();
        PromQLSynonymLookup.put(original, synonym);

        assertEquals(synonym, PromQLSynonymLookup.getSynonym(original));
        assertEquals(synonym, PromQLSynonymLookup.getSynonym(synonym));
        assertEquals("host", PromQLSynonymLookup.getSwappedTagsBySynonymName("cpu_usage").get("HST1"));
    }


    @Test
    void getOriginalName() {

        String original = "DC1_HST1_CPU.aze{data_center=DC1, host=HST1}";
        String synonym = "cpu_usage{data_center=DC1, host=HST1}";

        PromQLSynonymLookup.enable();
        PromQLSynonymLookup.put(original, synonym);

        assertEquals("DC1_HST1_CPU.aze", PromQLSynonymLookup.getOriginalName(synonym));
    }

    @Test
    void getNames() {

        String original = "DC1_HST1_CPU.aze{data_center=DC1, host=HST1}";
        String synonym = "cpu_usage{data_center=DC1, host=HST1}";

        String original1 = "DC1_HST1_MEM.aze{data_center=DC1, host=HST1}";
        String synonym1 = "memory_usage{data_center=DC1, host=HST1}";

        String original2 = "DC1_HST2_MEM.aze{data_center=DC1, host=HST2}";
        String synonym2 = "memory_usage{data_center=DC1, host=HST2}";

        PromQLSynonymLookup.enable();
        PromQLSynonymLookup.put(original, synonym);
        PromQLSynonymLookup.put(original1, synonym1);
        PromQLSynonymLookup.put(original2, synonym2);

        assertEquals(2, PromQLSynonymLookup.getSynonymsNames().size());
        assertTrue( PromQLSynonymLookup.getSynonymsNames().contains("memory_usage"));
        assertTrue( PromQLSynonymLookup.getSynonymsNames().contains("cpu_usage"));
    }


    @Test
    void testLoadingFromFile() throws IOException {

        String csvFilePath = this.getClass().getClassLoader().getResource("synonyms.csv").getPath();
        PromQLSynonymLookup.enable();
        PromQLSynonymLookup.loadCacheFromFile(csvFilePath, ';');

        assertEquals("ZE2345.AZE_123_AD.F_CV", PromQLSynonymLookup.getOriginal("DebitVolumique.H2_Prevision_strippage.CoefSuper_Z.ZE2345"));
        assertEquals("ZE2346.AZE_123_AD.F_CV", PromQLSynonymLookup.getOriginal("DebitVolumique.H2_Prevision_strippage.CoefSuper_Z.ZE2346"));
        assertEquals("ZE2347.AZE_123_AD.F_CV", PromQLSynonymLookup.getOriginal("DebitVolumique.H2_Prevision_strippage.CoefSuper_Z.ZE2347"));
        assertEquals("DebitVolumique.H2_Prevision_strippage.CoefSuper_Z.ZE2345", PromQLSynonymLookup.getSynonym("ZE2345.AZE_123_AD.F_CV"));
        assertEquals("DebitVolumique.H2_Prevision_strippage.CoefSuper_Z.ZE2346", PromQLSynonymLookup.getSynonym("ZE2346.AZE_123_AD.F_CV"));
        assertEquals("DebitVolumique.H2_Prevision_strippage.CoefSuper_Z.ZE2347", PromQLSynonymLookup.getSynonym("ZE2347.AZE_123_AD.F_CV"));

        String csvFilePath2 = this.getClass().getClassLoader().getResource("synonyms-with-tags.csv").getPath();
        PromQLSynonymLookup.enable();
        PromQLSynonymLookup.loadCacheFromFile(csvFilePath2, ',');

        assertEquals("cpu_prct_used{metric_id=ac8519bc-2985-4869-8aa8-c5fdfc68ec44}", PromQLSynonymLookup.getOriginal("cpu_prct_used_a"));
        assertEquals("cpu_prct_used{metric_id=ae320972-f612-4cef-8570-908250b8bd1a}", PromQLSynonymLookup.getOriginal("cpu_prct_used_b"));
        assertEquals("cpu_prct_used{metric_id=af33574c-c852-4b76-9e67-b19ad444250d}", PromQLSynonymLookup.getOriginal("cpu_prct_used_c"));
        assertEquals("cpu_prct_used{metric_id=b2c2b01e-2c54-4d05-9687-b69cab6cfc68}", PromQLSynonymLookup.getOriginal("cpu_prct_used_d"));


    }
}