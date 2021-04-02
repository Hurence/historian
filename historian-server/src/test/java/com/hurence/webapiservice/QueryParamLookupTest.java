package com.hurence.webapiservice;

import com.google.common.collect.ImmutableMap;
import com.hurence.webapiservice.http.api.grafana.promql.parameter.QueryParameter;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.Assert.*;

class QueryParamLookupTest {


    @Test
    void getOriginalAndSynonyms() {

        String original = "DC1_HST1_CPU";
        String synonym = "cpu_usage";

        QueryParamLookup.enable();
        QueryParamLookup.put(original, synonym);
        assertEquals(original, QueryParamLookup.getOriginal(original));
        assertEquals(original, QueryParamLookup.getOriginal(synonym));

        assertEquals(synonym, QueryParamLookup.getSynonym(original));
        assertEquals(synonym, QueryParamLookup.getSynonym(synonym));

        assertEquals("cpu_usage", QueryParamLookup.getSynonymName(original));
        assertEquals("cpu_usage", QueryParamLookup.getSynonymName(synonym));

    }


    @Test
    void getNameFromSynonymWithStrings() {

        String original = "DC1_HST1_CPU";
        String synonym = "cpu_usage{data_center=DC1, host=HST1}";

        QueryParamLookup.enable();
        QueryParamLookup.put(original, synonym);
        assertEquals(original, QueryParamLookup.getOriginal(original));
        assertEquals(original, QueryParamLookup.getOriginal(synonym));

        assertEquals(synonym, QueryParamLookup.getSynonym(original));
        assertEquals(synonym, QueryParamLookup.getSynonym(synonym));

        assertEquals("cpu_usage", QueryParamLookup.getSynonymName(original));
        assertEquals("cpu_usage", QueryParamLookup.getSynonymName(synonym));

    }

    @Test
    void getNameFromSynonymWithStringsAndTags() {

        String original = "DC1_HST1_CPU{data_center=DC1, host=HST1}";
        String synonym = "cpu_usage{data_center=DC1, host=HST1}";

        QueryParamLookup.enable();
        QueryParamLookup.put(original, synonym);
        assertEquals(original, QueryParamLookup.getOriginal(original));
        assertEquals(original, QueryParamLookup.getOriginal(synonym));

        assertEquals(synonym, QueryParamLookup.getSynonym(original));
        assertEquals(synonym, QueryParamLookup.getSynonym(synonym));

        assertEquals("cpu_usage", QueryParamLookup.getSynonymName(original));
        assertEquals("cpu_usage", QueryParamLookup.getSynonymName(synonym));
        assertEquals("cpu_usage", QueryParamLookup.getSynonymName("DC1_HST1_CPU"));

    }

    @Test
    void getTagsBySynonymName() {

        String original = "DC1_HST1_CPU";
        String synonym = "cpu_usage{data_center=DC1, host=HST1}";

        QueryParamLookup.enable();
        QueryParamLookup.put(original, synonym);

        QueryParamLookup.put("cpu_prct_used{metric_id=ac8519bc-2985-4869-8aa8-c5fdfc68ec44}", "cpu_prct_used_renamed{metric_id=ac8519bc-2985-4869-8aa8-c5fdfc68ec44}");
        QueryParamLookup.put("cpu_prct_used{metric_id=ae320972-f612-4cef-8570-908250b8bd1a}", "cpu_prct_used_renamed{metric_id=ae320972-f612-4cef-8570-908250b8bd1a}");
        QueryParamLookup.put("cpu_prct_used{metric_id=af33574c-c852-4b76-9e67-b19ad444250d}", "cpu_prct_used_renamed{metric_id=af33574c-c852-4b76-9e67-b19ad444250d}");
        QueryParamLookup.put("cpu_prct_used{metric_id=b2c2b01e-2c54-4d05-9687-b69cab6cfc68}", "cpu_prct_used_renamed{metric_id=b2c2b01e-2c54-4d05-9687-b69cab6cfc68}");

        assertEquals("data_center", QueryParamLookup.getSwappedTagsBySynonymName("cpu_usage").get("DC1"));
        assertEquals("host", QueryParamLookup.getSwappedTagsBySynonymName("cpu_usage").get("HST1"));
        assertNull(QueryParamLookup.getSwappedTagsBySynonymName("plik"));

        assertEquals("metric_id", QueryParamLookup.getSwappedTagsBySynonymName("cpu_prct_used_renamed").get("ac8519bc-2985-4869-8aa8-c5fdfc68ec44" ));
        assertEquals(4, QueryParamLookup.getSwappedTagsBySynonymName("cpu_prct_used_renamed").values().size());
    }

    @Test
    void getTagsByNameWithDot() {

        String original = "DC1_HST1_CPU.aze{data_center=DC1, host=HST1}";
        String synonym = "cpu_usage{data_center=DC1, host=HST1}";

        QueryParamLookup.enable();
        QueryParamLookup.put(original, synonym);

        assertEquals(synonym, QueryParamLookup.getSynonym(original));
        assertEquals(synonym, QueryParamLookup.getSynonym(synonym));
        assertEquals("host", QueryParamLookup.getSwappedTagsBySynonymName("cpu_usage").get("HST1"));
    }


    @Test
    void getOriginalName() {

        String original = "DC1_HST1_CPU.aze{data_center=DC1, host=HST1}";
        String synonym = "cpu_usage{data_center=DC1, host=HST1}";

        QueryParamLookup.enable();
        QueryParamLookup.put(original, synonym);

        assertEquals("DC1_HST1_CPU.aze", QueryParamLookup.getOriginalName(synonym));
    }

    @Test
    void getNames() {

        String original = "DC1_HST1_CPU.aze{data_center=DC1, host=HST1}";
        String synonym = "cpu_usage{data_center=DC1, host=HST1}";

        String original1 = "DC1_HST1_MEM.aze{data_center=DC1, host=HST1}";
        String synonym1 = "memory_usage{data_center=DC1, host=HST1}";

        String original2 = "DC1_HST2_MEM.aze{data_center=DC1, host=HST2}";
        String synonym2 = "memory_usage{data_center=DC1, host=HST2}";

        QueryParamLookup.enable();
        QueryParamLookup.put(original, synonym);
        QueryParamLookup.put(original1, synonym1);
        QueryParamLookup.put(original2, synonym2);

        assertEquals(2, QueryParamLookup.getSynonymsNames().size());
        assertTrue( QueryParamLookup.getSynonymsNames().contains("memory_usage"));
        assertTrue( QueryParamLookup.getSynonymsNames().contains("cpu_usage"));
    }


    @Test
    void testLoadingFromFile() throws IOException {

        String csvFilePath = this.getClass().getClassLoader().getResource("synonyms.csv").getPath();
        QueryParamLookup.enable();
        QueryParamLookup.loadCacheFromFile(csvFilePath, ';');

        assertEquals("ZE2345.AZE_123_AD.F_CV", QueryParamLookup.getOriginal("DebitVolumique.H2_Prevision_strippage.CoefSuper_Z.ZE2345"));
        assertEquals("ZE2346.AZE_123_AD.F_CV", QueryParamLookup.getOriginal("DebitVolumique.H2_Prevision_strippage.CoefSuper_Z.ZE2346"));
        assertEquals("ZE2347.AZE_123_AD.F_CV", QueryParamLookup.getOriginal("DebitVolumique.H2_Prevision_strippage.CoefSuper_Z.ZE2347"));
        assertEquals("DebitVolumique.H2_Prevision_strippage.CoefSuper_Z.ZE2345", QueryParamLookup.getSynonym("ZE2345.AZE_123_AD.F_CV"));
        assertEquals("DebitVolumique.H2_Prevision_strippage.CoefSuper_Z.ZE2346", QueryParamLookup.getSynonym("ZE2346.AZE_123_AD.F_CV"));
        assertEquals("DebitVolumique.H2_Prevision_strippage.CoefSuper_Z.ZE2347", QueryParamLookup.getSynonym("ZE2347.AZE_123_AD.F_CV"));

        String csvFilePath2 = this.getClass().getClassLoader().getResource("synonyms-with-tags.csv").getPath();
        QueryParamLookup.enable();
        QueryParamLookup.loadCacheFromFile(csvFilePath2, ',');

        assertEquals("cpu_prct_used{metric_id=ac8519bc-2985-4869-8aa8-c5fdfc68ec44}", QueryParamLookup.getOriginal("cpu_prct_used_a"));
        assertEquals("cpu_prct_used{metric_id=ae320972-f612-4cef-8570-908250b8bd1a}", QueryParamLookup.getOriginal("cpu_prct_used_b"));
        assertEquals("cpu_prct_used{metric_id=af33574c-c852-4b76-9e67-b19ad444250d}", QueryParamLookup.getOriginal("cpu_prct_used_c"));
        assertEquals("cpu_prct_used{metric_id=b2c2b01e-2c54-4d05-9687-b69cab6cfc68}", QueryParamLookup.getOriginal("cpu_prct_used_d"));


    }

    @Test
    void bigTest() throws IOException {
        QueryParamLookup.enable();
        QueryParamLookup.loadCacheFromFile("/Users/tom/Documents/workspace/ifpen/data-historian/conf/synonyms.csv", ';');

        //U204.U204_FI08.F_CV;DebitVolumique{unit="U204", location="Gaz_BP", type="Mesure_PV"}
        //U204.U204_FI13.F_CV;DebitVolumique{unit="U204", location="Gaz_HP", type="Mesure_PV"}

        assertEquals("DebitVolumique", QueryParamLookup.getSynonymName("U204.U204_FI08.F_CV"));
        assertEquals("U204.U204_FI08.F_CV",QueryParamLookup.getOriginal("DebitVolumique{unit=\"U204\", location=\"Gaz_BP\", type=\"Mesure_PV\"}"));
        assertEquals("U204.U204_FI08.F_CV",QueryParamLookup.getOriginal("DebitVolumique{location=\"Gaz_BP\", unit=\"U204\", type=\"Mesure_PV\"}"));
    }

}