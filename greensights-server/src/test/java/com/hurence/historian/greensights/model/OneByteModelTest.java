package com.hurence.historian.greensights.model;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class OneByteModelTest {

	@Test
	public void kwhComputationTest(){

		EnergyImpactMetric metric = new EnergyImpactMetric();

		metric.setPageSizeInBytes(1323228);
		metric.setDeviceCategory("desktop");
		metric.setCountry("Austria");

		Assertions.assertEquals( 8.976038946666668E-4, metric.getEnergyImpactInKwh());
		Assertions.assertEquals( 2.4773867492800005E-4, metric.getCo2EqInKg());

		metric.setCountry("France");
		Assertions.assertEquals( 8.976038946666668E-4, metric.getEnergyImpactInKwh());
		Assertions.assertEquals( 3.141613631333334E-5, metric.getCo2EqInKg());

	}

    @Test
    public void carbonIntensityFactorFromCountryTest(){

        Assertions.assertEquals(OneByteModel.getCarbonIntensityFactor("Austria"),
				OneByteModel.CARBON_INTENSITY_FACTORS_EU);

		Assertions.assertEquals(OneByteModel.getCarbonIntensityFactor("Brazil"),
				OneByteModel.CARBON_INTENSITY_FACTORS_WORLD);
		Assertions.assertEquals(OneByteModel.getCarbonIntensityFactor("United States"),
				OneByteModel.CARBON_INTENSITY_FACTORS_US);
		Assertions.assertEquals(OneByteModel.getCarbonIntensityFactor("Canada"),
				OneByteModel.CARBON_INTENSITY_FACTORS_US);
		Assertions.assertEquals(OneByteModel.getCarbonIntensityFactor("China"),
				OneByteModel.CARBON_INTENSITY_FACTORS_CH);
		Assertions.assertEquals(OneByteModel.getCarbonIntensityFactor("Czechia"),
				OneByteModel.CARBON_INTENSITY_FACTORS_EU);
		Assertions.assertEquals(OneByteModel.getCarbonIntensityFactor("Finland"),
				OneByteModel.CARBON_INTENSITY_FACTORS_EU);
		Assertions.assertEquals(OneByteModel.getCarbonIntensityFactor("France"),
				OneByteModel.CARBON_INTENSITY_FACTORS_FR);
    }
}