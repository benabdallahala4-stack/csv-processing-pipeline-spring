package com.benabdallah.csvpipeline;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses; import com.tngtech.archunit.core.importer.ImportOption; import com.tngtech.archunit.junit.*;
@AnalyzeClasses(packages="com.benabdallah.csvpipeline",importOptions=ImportOption.DoNotIncludeTests.class) class ArchitectureTest {
 @ArchTest static final com.tngtech.archunit.lang.ArchRule domain_is_pure=noClasses().that().resideInAPackage("..upload..").should().dependOnClassesThat().resideInAnyPackage("org.springframework..","software.amazon..","org.apache.kafka..","..adapter..");
 @ArchTest static final com.tngtech.archunit.lang.ArchRule application_is_framework_free=noClasses().that().resideInAPackage("..application..").should().dependOnClassesThat().resideInAnyPackage("org.springframework..","software.amazon..","org.apache.kafka..","..adapter..");
}
