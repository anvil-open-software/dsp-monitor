/* Copyright 2018, Dematic, Corp.
   Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT */

package com.dematic.labs.analytics.monitor.spark;

import com.dematic.labs.analytics.monitor.spark.dropwizard.DropwizardSparkExports;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 *
 */
public final class DropWizardNameConversionTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(DropWizardNameConversionTest.class);

    @Test
    public void shortenNameTest() {
        String jvm_heap_metric="application_1495474513333_0001.2.jvm.heap.committed";
        String shortened_name= DropwizardSparkExports.sanitizeMetricName(jvm_heap_metric);
        Assert.assertEquals("jvm_heap_committed", shortened_name);

        String notShortenedName="anyother.nuts.eaten";
        String notShortenedNameWithUnderscore="anyother_nuts_eaten";
        shortened_name= DropwizardSparkExports.sanitizeMetricName(notShortenedName);
        Assert.assertEquals(shortened_name, notShortenedNameWithUnderscore);

        String multicaseName="anyOther.Nuts.Eaten";
        shortened_name= DropwizardSparkExports.sanitizeMetricName(notShortenedName);
        Assert.assertEquals(shortened_name, notShortenedNameWithUnderscore);


    }


}