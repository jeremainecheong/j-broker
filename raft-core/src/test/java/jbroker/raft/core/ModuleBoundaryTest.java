package jbroker.raft.core;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.Test;

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
