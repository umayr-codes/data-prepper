/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericApplicationContext;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class PluginBeanFactoryProviderTest {

    private ApplicationContext context;

    @BeforeEach
    void setUp() {
        context = mock(ApplicationContext.class);
    }

    private PluginBeanFactoryProvider createObjectUnderTest() {
        return new PluginBeanFactoryProvider(context);
    }

    @Test
    void testPluginBeanFactoryProviderUsesParentContext() {

        doReturn(context).when(context).getParent();

        createObjectUnderTest();

        verify(context).getParent();
    }

    @Test
    void testPluginBeanFactoryProviderRequiresContext() {
        context = null;
        assertThrows(NullPointerException.class, () -> createObjectUnderTest());
    }

    @Test
    void testPluginBeanFactoryProviderRequiresParentContext() {
        context = mock(ApplicationContext.class);

        assertThrows(NullPointerException.class, () -> createObjectUnderTest());
    }

    @Test
    void testPluginBeanFactoryProviderGetReturnsBeanFactory() {
        doReturn(context).when(context).getParent();

        final PluginBeanFactoryProvider beanFactoryProvider = createObjectUnderTest();

        verify(context).getParent();
        assertThat(beanFactoryProvider.get(), is(instanceOf(BeanFactory.class)));
    }

    @Test
    void testPluginBeanFactoryProviderGetReturnsUniqueBeanFactory() {
        doReturn(context).when(context).getParent();

        final PluginBeanFactoryProvider beanFactoryProvider = createObjectUnderTest();
        final BeanFactory isolatedBeanFactoryA = beanFactoryProvider.get();
        final BeanFactory isolatedBeanFactoryB = beanFactoryProvider.get();

        verify(context).getParent();
        assertThat(isolatedBeanFactoryA, not(sameInstance(isolatedBeanFactoryB)));
    }

    @Test
    void getSharedPluginApplicationContext_returns_created_ApplicationContext() {
        doReturn(context).when(context).getParent();
        final GenericApplicationContext actualContext = createObjectUnderTest().getSharedPluginApplicationContext();

        assertThat(actualContext, notNullValue());
        assertThat(actualContext.getParent(), equalTo(context));
    }

    @Test
    void getSharedPluginApplicationContext_called_multiple_times_returns_same_instance() {
        doReturn(context).when(context).getParent();
        final PluginBeanFactoryProvider objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.getSharedPluginApplicationContext(), sameInstance(objectUnderTest.getSharedPluginApplicationContext()));
    }
}