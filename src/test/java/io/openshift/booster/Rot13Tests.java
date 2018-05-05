package io.openshift.booster;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.CoreMatchers.nullValue;

/**
 * Check the behavior of the application when running in OpenShift.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
//@RunWith(Arquillian.class)
public class Rot13Tests {
    @Test
    public void testAThatRot13OfNullIsNull() {
        assertThat(Rot13.rotate(null), is(nullValue()));    
    }
    
    @Test
    public void testBTheHelloWordIsRotatedCorrectly() {
        assertThat(Rot13.rotate("Hello World"), is("Uryyb Jbeyq"));
    }
    
    @Test
    public void testCTheDoubleRotationIsIdempotent() {
        assertThat(Rot13.rotate(Rot13.rotate("Hello World")), is("Hello World"));
    }

    @Test
    public void testDRotationIgnoresNonAlpha() {
        assertThat(Rot13.rotate("1234567890!@#$%^&*()"), is("1234567890!@#$%^&*()"));
    }
}