package jbroker.raft.core;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.Test;

/**
 * Enforces {@code raft-core} module boundaries: no Spring, no gRPC, no Jakarta.
 *
 * <p>Each rule uses {@code .allowEmptyShould(true)} because ArchUnit 1.3.0 errors when a rule
 * evaluates zero classes. The {@code jbroker.raft.core} package contains only {@code package-info}
 * during Tasks 9–10; real classes arrive from Task 11 onward. Do not remove the flag — it is
 * harmless once classes exist and guards against the rule silently failing if all sources are
 * deleted.
 */
class ModuleBoundaryTest {

    @Test
    void raftCoreMustNotDependOnSpring() {
        var classes = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("jbroker.raft.core");

        noClasses()
                .should()
                .dependOnClassesThat()
                .resideInAnyPackage("org.springframework..")
                .allowEmptyShould(true)
                .check(classes);
    }

    @Test
    void raftCoreMustNotDependOnGrpc() {
        var classes = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("jbroker.raft.core");

        noClasses()
                .should()
                .dependOnClassesThat()
                .resideInAnyPackage("io.grpc..")
                .allowEmptyShould(true)
                .check(classes);
    }

    @Test
    void raftCoreMustNotDependOnJakarta() {
        var classes = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("jbroker.raft.core");

        noClasses()
                .should()
                .dependOnClassesThat()
                .resideInAnyPackage("jakarta..")
                .allowEmptyShould(true)
                .check(classes);
    }
}
