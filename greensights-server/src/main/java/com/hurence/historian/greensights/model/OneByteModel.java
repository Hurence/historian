package com.hurence.historian.greensights.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 *
 * According to
 * the [1byteModel](https://theshiftproject.org/wp-content/uploads/2019/10/Lean-ICT-Materials-Liens-%C3%A0-t%C3%A9l%C3%A9charger-r%C3%A9par%C3%A9-le-29-10-2019.pdf)
 * given by TheshiftProject
 */
public class OneByteModel {

    public static double GESgCO2ForOneKmByCar = 220.0;
    public static double GESgCO2ForOneChargedSmartphone = 8.3;


    // Energy Impact due to Data Centers (in kWh/byte)
    public static double ENERGY_IMPACT_DUE_TO_DATACENTERS = 7.2E-11;



    // Energy impact for FAN Wired (in kWh/byte)
    public static double ENERGY_IMPACT_FOR_FAN_WIRED = 4.29E-10;

    // Energy impact for FAN WIFI (in kWh/byte)
    public static double ENERGY_IMPACT_FOR_FAN_WIFI = 1.52E-10;

    // Energy impact for Mobile Network (in kWh/byte)
    public static double ENERGY_IMPACT_FOR_MOBILE_NETWORK = 8.84E-10;



    // Energy impact for device Smartphone (in kWh/min)
    public static double ENERGY_IMPACT_DEVICE_SMARTPHONE = 1.1E-4;

    // Energy impact for device desktop (in kWh/min)
    public static double ENERGY_IMPACT_DEVICE_DESKTOP = 3.2E-4;



    // European Union
    public static double CARBON_INTENSITY_FACTORS_EU = 0.276;

    // United States
    public static double CARBON_INTENSITY_FACTORS_US = 0.493;

    // China
    public static double CARBON_INTENSITY_FACTORS_CH = 0.681;

    // World
    public static double CARBON_INTENSITY_FACTORS_WORLD = 0.519;

    // France
    public static double CARBON_INTENSITY_FACTORS_FR = 0.035;


    /**
     * in kgCO2e/kWh from LeanICT REN
     *
     * @param countryName
     * @return carbon intensity factor in kgCO2e/kWh
     */
    public static double getCarbonIntensityFactor(String countryName){

        String lcCountryName = countryName.toLowerCase(Locale.ROOT);

        List<String> euCountries = new ArrayList<>();
        for (String p : Arrays.asList("Albania", "Andorra", "Austria", "Belgium", "Bulgaria", "Croatia", "Cyprus", "Czechia", "Denmark", "Estonia", "Finland", "France", "Germany", "Greece", "Hungary", "Ireland", "Italy", "Latvia", "Lithuania", "Luxembourg", "Malta", "Netherlands", "Poland", "Portugal", "Romania", "Slovakia", "Slovenia", "Spain", "Sweden", "Switzerland")) {
            String s = p.toLowerCase(Locale.ROOT);
            euCountries.add(s);
        }


        if(lcCountryName.equals("china"))
            return CARBON_INTENSITY_FACTORS_CH;

        else if(lcCountryName.equals("france"))
            return CARBON_INTENSITY_FACTORS_FR;

        else if(lcCountryName.equals("united states") || lcCountryName.equals("canada"))
            return CARBON_INTENSITY_FACTORS_US;

        else if(euCountries.contains(lcCountryName))
            return CARBON_INTENSITY_FACTORS_EU;

        else return CARBON_INTENSITY_FACTORS_WORLD;

    }
}
