package com.taosdata.jdbc.annotation;

import org.junit.internal.AssumptionViolatedException;
import org.junit.internal.runners.model.EachTestNotifier;
import org.junit.runner.notification.RunNotifier;
import org.junit.runner.notification.StoppedByUserException;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

public class CatalogRunner extends BlockJUnit4ClassRunner {

    public CatalogRunner(Class<?> testClass) throws InitializationError {
        super(testClass);
    }

    @Override
    public void run(RunNotifier notifier) {
        //add user-defined listener
        notifier.addListener(new CatalogListener());
        EachTestNotifier testNotifier = new EachTestNotifier(notifier, getDescription());

        notifier.fireTestRunStarted(getDescription());

        try {
            Statement statement = classBlock(notifier);
            statement.evaluate();
        } catch (AssumptionViolatedException av) {
            testNotifier.addFailedAssumption(av);
        } catch (StoppedByUserException exception) {
            throw exception;
        } catch (Throwable e) {
            testNotifier.addFailure(e);
        }
    }
}