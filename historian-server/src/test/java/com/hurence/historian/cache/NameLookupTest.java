package com.hurence.historian.cache;

import com.hurence.webapiservice.NameLookup;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class NameLookupTest {

    @Test
    void invalidate() {
        NameLookup.enable();
        NameLookup.putTag("a", "a1");
        assertTrue( NameLookup.getNameFromSynonym("a1").isPresent());
        assertEquals("a", NameLookup.getNameFromSynonym("a1").get());

        NameLookup.invalidate();
        assertFalse( NameLookup.getNameFromSynonym("a1").isPresent());
    }

    @Test
    void loadCacheFromFile() throws IOException {
        NameLookup.enable();
        // ZE2347.AZE_123_AD.F_CV;DebitVolumique.H2_Prevision_strippage.CoefSuper_Z.ZE2347
        String csvFilePath = this.getClass().getClassLoader().getResource("synonyms.csv").getPath();
        NameLookup.loadCacheFromFile(csvFilePath, ';');
        assertTrue( NameLookup.getNameFromSynonym("DebitVolumique.H2_Prevision_strippage.CoefSuper_Z.ZE2347").isPresent());
        assertEquals("ZE2347.AZE_123_AD.F_CV", NameLookup.getNameFromSynonym("DebitVolumique.H2_Prevision_strippage.CoefSuper_Z.ZE2347").get());
    }

    @Test
    void getTag() {
        NameLookup.enable();
        NameLookup.putTag("a", "a1");
        assertTrue( NameLookup.getNameFromSynonym("a1").isPresent());
        assertEquals("a", NameLookup.getNameFromSynonym("a1").get());
    }
}