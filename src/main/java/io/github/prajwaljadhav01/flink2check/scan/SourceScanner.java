package io.github.prajwaljadhav01.flink2check.scan;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ParseResult;
import com.github.javaparser.ParserConfiguration;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.ImportDeclaration;
import com.github.javaparser.ast.Node;
import com.github.javaparser.ast.body.FieldDeclaration;
import com.github.javaparser.ast.body.MethodDeclaration;
import com.github.javaparser.ast.body.Parameter;
import com.github.javaparser.ast.body.VariableDeclarator;
import com.github.javaparser.ast.expr.Expression;
import com.github.javaparser.ast.expr.FieldAccessExpr;
import com.github.javaparser.ast.expr.MethodCallExpr;
import com.github.javaparser.ast.expr.NameExpr;
import com.github.javaparser.ast.expr.StringLiteralExpr;
import com.github.javaparser.ast.type.ClassOrInterfaceType;
import com.github.javaparser.ast.type.Type;
import io.github.prajwaljadhav01.flink2check.model.Finding;
import io.github.prajwaljadhav01.flink2check.model.Report;
import io.github.prajwaljadhav01.flink2check.model.Severity;
import io.github.prajwaljadhav01.flink2check.rules.Rules;
import io.github.prajwaljadhav01.flink2check.rules.Rules.RemovedClass;
import io.github.prajwaljadhav01.flink2check.rules.Rules.RemovedMember;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Scans Java (and, more lightly, Scala) sources for APIs that Flink 2.x removed.
 *
 * <p>It works on syntax only, without compiling the project, so it resolves names through the
 * file's imports and the declared types of local variables, fields and parameters. A removed
 * method is reported as an ERROR when the receiver's type was resolved, and as a WARNING when it
 * was guessed.
 */
public final class SourceScanner {

    /** Methods whose name alone says nothing; never reported from a guessed receiver. */
    private static final Set<String> GENERIC_NAMES = Set.of(
            "of", "with", "open", "apply", "collect", "getClass", "configure", "getKind", "defaultValue",
            "parse", "getString", "setString", "getBytes", "setBytes", "newBuilder", "cast", "getFilePath",
            "iterate", "close", "build", "create", "get", "set", "toString", "equals", "hashCode",
            "getJobName", "getTaskName", "getJobId", "getTtl", "setTtl", "isGeneric", "toProperties",
            "getCheckpointId", "fromConfig", "SYNTHETIC", "canEqual", "noDefaultValue", "getMinValue",
            "getMaxValue", "getFloat", "getDouble", "getLong", "getInteger", "getBoolean", "setFloat",
            "setDouble", "setLong", "setInteger", "setBoolean", "setClass");

    /** Owners that user code usually reaches through a getter instead of an import. */
    private static final Map<String, String> KNOWN_OWNERS = Map.ofEntries(
            Map.entry("StreamExecutionEnvironment", "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment"),
            Map.entry("CheckpointConfig", "org.apache.flink.streaming.api.environment.CheckpointConfig"),
            Map.entry("ExecutionConfig", "org.apache.flink.api.common.ExecutionConfig"),
            Map.entry("RuntimeContext", "org.apache.flink.api.common.functions.RuntimeContext"),
            Map.entry("Configuration", "org.apache.flink.configuration.Configuration"),
            Map.entry("DataStream", "org.apache.flink.streaming.api.datastream.DataStream"),
            Map.entry("KeyedStream", "org.apache.flink.streaming.api.datastream.KeyedStream"),
            Map.entry("StreamTableEnvironment", "org.apache.flink.table.api.bridge.java.StreamTableEnvironment"),
            Map.entry("TableEnvironment", "org.apache.flink.table.api.TableEnvironment"),
            Map.entry("TableConfig", "org.apache.flink.table.api.TableConfig"),
            Map.entry("StateTtlConfig", "org.apache.flink.api.common.state.StateTtlConfig"),
            Map.entry("EmbeddedRocksDBStateBackend", "org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend"));

    /** Getter name on the receiver chain -> the type it returns. */
    private static final Map<String, String> RECEIVER_HINTS = Map.of(
            "getRuntimeContext", "RuntimeContext",
            "getConfig", "ExecutionConfig",
            "getCheckpointConfig", "CheckpointConfig",
            "getExecutionEnvironment", "StreamExecutionEnvironment",
            "getTableConfig", "TableConfig");

    private static final Pattern SCALA_IMPORT = Pattern.compile("^\\s*import\\s+(org\\.apache\\.flink\\.[\\w.]+)");

    private final Rules rules;
    private final Report report;
    private final JavaParser parser;
    /** Owners that have at least one fully removed (not overload-only) method name. */
    private final Map<String, Set<String>> fullyRemovedMethodNames = new HashMap<>();
    /** Operator uids set in code with .uid("..."), used to name operators in a savepoint. */
    private final Set<String> uids = new java.util.TreeSet<>();

    public SourceScanner(Rules rules, Report report) {
        this.rules = rules;
        this.report = report;
        ParserConfiguration cfg = new ParserConfiguration()
                .setLanguageLevel(ParserConfiguration.LanguageLevel.JAVA_17)
                .setCharacterEncoding(StandardCharsets.UTF_8);
        this.parser = new JavaParser(cfg);
        for (String owner : rules.ownersWithRemovedMembers()) {
            for (RemovedMember m : rules.removedMembers(owner)) {
                if (m.kind().equals("method") && !m.overloadOnly()) {
                    fullyRemovedMethodNames.computeIfAbsent(owner, k -> new HashSet<>()).add(m.name());
                }
            }
        }
    }

    public Set<String> uids() {
        return uids;
    }

    public int scan(Path root) {
        List<Path> javaFiles = ProjectFiles.find(root, p -> ProjectFiles.nameOf(p).endsWith(".java"));
        for (Path f : javaFiles) {
            scanJava(root, f);
        }
        List<Path> scalaFiles = ProjectFiles.find(root, p -> ProjectFiles.nameOf(p).endsWith(".scala"));
        for (Path f : scalaFiles) {
            scanScala(root, f);
        }
        return javaFiles.size() + scalaFiles.size();
    }

    // ---------------------------------------------------------------- Java

    private void scanJava(Path root, Path file) {
        String where = ProjectFiles.display(root, file);
        ParseResult<CompilationUnit> result;
        try {
            result = parser.parse(file);
        } catch (IOException e) {
            report.add(info("code", "parse-error", "Could not read source file", e.getMessage(), where, 0));
            return;
        }
        if (result.getResult().isEmpty()) {
            report.add(info("code", "parse-error", "Could not parse source file",
                    result.getProblems().isEmpty() ? "unknown problem" : result.getProblems().get(0).getMessage(),
                    where, 0));
            return;
        }
        new FileScan(result.getResult().get(), where).run();
    }

    /** State for one compilation unit. */
    private final class FileScan {
        private final CompilationUnit cu;
        private final String where;
        private final Map<String, String> imported = new HashMap<>();      // simple -> fqcn
        private final List<String> wildcardPackages = new ArrayList<>();
        private final Map<String, String> variableTypes = new HashMap<>(); // var -> simple type
        private final Set<String> reportedClasses = new HashSet<>();
        private boolean importsFlink;

        FileScan(CompilationUnit cu, String where) {
            this.cu = cu;
            this.where = where;
        }

        void run() {
            for (ImportDeclaration imp : cu.getImports()) {
                handleImport(imp);
            }
            collectVariableTypes();
            cu.findAll(ClassOrInterfaceType.class).forEach(this::checkTypeReference);
            cu.findAll(NameExpr.class).forEach(n -> checkSimpleName(n.getNameAsString(), n));
            cu.findAll(FieldAccessExpr.class).forEach(this::checkFieldAccess);
            cu.findAll(MethodCallExpr.class).forEach(this::checkMethodCall);
            cu.findAll(MethodDeclaration.class).forEach(this::checkOverride);
            if (importsFlink) {
                cu.findAll(StringLiteralExpr.class).forEach(this::checkConfigKeyLiteral);
            }
        }

        private void handleImport(ImportDeclaration imp) {
            String name = imp.getNameAsString();
            if (name.startsWith("org.apache.flink")) {
                importsFlink = true;
            }
            int line = line(imp);
            if (imp.isStatic()) {
                // import static Owner.member  /  import static Owner.*
                String owner = imp.isAsterisk() ? name : name.substring(0, name.lastIndexOf('.'));
                reportClassIfRemoved(owner, line);
                if (!imp.isAsterisk()) {
                    String member = name.substring(name.lastIndexOf('.') + 1);
                    for (RemovedMember m : rules.removedMembers(owner)) {
                        if (m.name().equals(member) && !m.overloadOnly()) {
                            reportMember(m, line, true);
                        }
                    }
                }
                return;
            }
            if (imp.isAsterisk()) {
                wildcardPackages.add(name);
                rules.removedPackageGroup(name).ifPresent(g -> reportPackage(name, g, line));
                // a wildcard over a removed class's nested classes: import Outer.*
                reportClassIfRemoved(name, line);
                return;
            }
            imported.put(name.substring(name.lastIndexOf('.') + 1), name);
            if (rules.removedPackageGroup(name).isPresent()) {
                reportPackage(name, rules.removedPackageGroup(name).get(), line);
                return;
            }
            reportClassIfRemoved(name, line);
        }

        private void collectVariableTypes() {
            cu.findAll(VariableDeclarator.class).forEach(v -> {
                String t = simpleTypeName(v.getType());
                if ("var".equals(t)) {
                    t = v.getInitializer().flatMap(this::guessTypeOf).orElse(null);
                }
                if (t != null) {
                    variableTypes.put(v.getNameAsString(), t);
                }
            });
            cu.findAll(Parameter.class).forEach(p -> variableTypes.put(p.getNameAsString(), simpleTypeName(p.getType())));
            cu.findAll(FieldDeclaration.class).forEach(f -> f.getVariables().forEach(
                    v -> variableTypes.putIfAbsent(v.getNameAsString(), simpleTypeName(v.getType()))));
        }

        private Optional<String> guessTypeOf(Expression init) {
            if (init instanceof MethodCallExpr call) {
                if (RECEIVER_HINTS.containsKey(call.getNameAsString())) {
                    return Optional.of(RECEIVER_HINTS.get(call.getNameAsString()));
                }
                if (call.getScope().isPresent() && call.getScope().get() instanceof NameExpr n
                        && Character.isUpperCase(n.getNameAsString().charAt(0))
                        && (call.getNameAsString().equals("create") || call.getNameAsString().startsWith("get"))) {
                    return Optional.of(n.getNameAsString());
                }
            }
            if (init.isObjectCreationExpr()) {
                return Optional.of(init.asObjectCreationExpr().getType().getNameAsString());
            }
            return Optional.empty();
        }

        private String simpleTypeName(Type t) {
            if (t.isClassOrInterfaceType()) {
                return t.asClassOrInterfaceType().getNameAsString();
            }
            return t.asString();
        }

        /** Simple type name -> fully qualified name, using imports, wildcard imports and well-known owners. */
        private Optional<String> resolve(String simple) {
            if (imported.containsKey(simple)) {
                return Optional.of(imported.get(simple));
            }
            for (String pkg : wildcardPackages) {
                String candidate = pkg + "." + simple;
                if (rules.removedClass(candidate).isPresent() || !rules.removedMembers(candidate).isEmpty()
                        || KNOWN_OWNERS.containsValue(candidate)) {
                    return Optional.of(candidate);
                }
            }
            if (importsFlink && KNOWN_OWNERS.containsKey(simple)) {
                return Optional.of(KNOWN_OWNERS.get(simple));
            }
            return Optional.empty();
        }

        private void checkTypeReference(ClassOrInterfaceType t) {
            String text = t.getNameWithScope();
            if (t.getScope().isEmpty()) {
                checkSimpleName(t.getNameAsString(), t);
                return;
            }
            checkDottedName(text, t);
        }

        private void checkSimpleName(String simple, Node node) {
            if (imported.containsKey(simple)) {
                return; // handled at the import line
            }
            for (String pkg : wildcardPackages) {
                RemovedClass rc = rules.removedInPackage(pkg).get(simple);
                if (rc != null) {
                    reportClass(rc, line(node));
                    return;
                }
            }
        }

        private void checkDottedName(String text, Node node) {
            // Fully qualified usage, e.g. org.apache.flink.api.common.time.Time
            if (text.startsWith("org.apache.flink.")) {
                rules.removedPackageGroup(text).ifPresent(g -> reportPackage(text, g, line(node)));
                reportClassIfRemoved(text, line(node));
                return;
            }
            // Outer.Inner where Outer is imported, e.g. CheckpointConfig.ExternalizedCheckpointCleanup
            int dot = text.indexOf('.');
            String first = text.substring(0, dot);
            Optional<String> outer = resolve(first);
            outer.ifPresent(o -> reportClassIfRemoved(o + text.substring(dot), line(node)));
        }

        private void checkFieldAccess(FieldAccessExpr fa) {
            String scopeText = fa.getScope().toString();
            if (scopeText.contains("(")) {
                return;
            }
            // Treat "A.B" (the scope plus name) as a possible nested class reference first.
            if (Character.isUpperCase(fa.getNameAsString().charAt(0)) && !isConstantName(fa.getNameAsString())) {
                checkDottedName(fa.toString(), fa);
            }
            // Removed static field: Owner.FIELD
            Optional<String> owner = scopeText.startsWith("org.apache.flink.")
                    ? Optional.of(scopeText)
                    : (scopeText.contains(".") ? resolveDotted(scopeText) : resolve(scopeText));
            owner.ifPresent(o -> {
                for (RemovedMember m : rules.removedMembers(o)) {
                    if (m.kind().equals("field") && m.name().equals(fa.getNameAsString())) {
                        reportMember(m, line(fa), true);
                    }
                }
            });
        }

        private Optional<String> resolveDotted(String text) {
            int dot = text.indexOf('.');
            return resolve(text.substring(0, dot)).map(o -> o + text.substring(dot));
        }

        private boolean isConstantName(String s) {
            return s.equals(s.toUpperCase()) && s.length() > 1;
        }

        private void checkMethodCall(MethodCallExpr call) {
            String name = call.getNameAsString();
            if (name.equals("uid") && call.getArguments().size() == 1 && call.getArgument(0).isStringLiteralExpr()) {
                uids.add(call.getArgument(0).asStringLiteralExpr().getValue());
            }
            Optional<Expression> scope = call.getScope();
            Optional<String> owner = Optional.empty();
            boolean resolved = false;
            if (scope.isPresent()) {
                Expression s = scope.get();
                if (s instanceof NameExpr n) {
                    String id = n.getNameAsString();
                    if (variableTypes.containsKey(id)) {
                        owner = resolve(variableTypes.get(id));
                    } else if (Character.isUpperCase(id.charAt(0))) {
                        owner = resolve(id); // static call on a class
                    }
                } else if (s instanceof MethodCallExpr inner && RECEIVER_HINTS.containsKey(inner.getNameAsString())) {
                    owner = resolve(RECEIVER_HINTS.get(inner.getNameAsString()));
                } else if (s instanceof FieldAccessExpr f && variableTypes.containsKey(f.getNameAsString())) {
                    owner = resolve(variableTypes.get(f.getNameAsString()));
                }
                resolved = owner.isPresent();
            }
            if (resolved) {
                String o = owner.get();
                if (rules.removedClass(o).isPresent()) {
                    return; // the class itself is already reported
                }
                for (RemovedMember m : rules.removedMembers(o)) {
                    if (m.kind().equals("method") && m.name().equals(name) && !m.overloadOnly()) {
                        reportMember(m, line(call), true);
                        return;
                    }
                }
                return;
            }
            if (!importsFlink || GENERIC_NAMES.contains(name)) {
                return;
            }
            // Receiver unknown: only flag names that vanished entirely from a commonly reached owner.
            for (String knownOwner : KNOWN_OWNERS.values()) {
                Set<String> names = fullyRemovedMethodNames.get(knownOwner);
                if (names != null && names.contains(name)) {
                    for (RemovedMember m : rules.removedMembers(knownOwner)) {
                        if (m.name().equals(name)) {
                            reportMember(m, line(call), false);
                            return;
                        }
                    }
                }
            }
        }

        /** RichFunction.open(Configuration) is gone; overrides of it silently stop being called or fail to compile. */
        private void checkOverride(MethodDeclaration md) {
            if (!md.getNameAsString().equals("open") || md.getParameters().size() != 1) {
                return;
            }
            String type = simpleTypeName(md.getParameter(0).getType());
            Optional<String> fq = type.contains(".") ? Optional.of(type) : resolve(type);
            if (fq.isPresent() && fq.get().equals("org.apache.flink.configuration.Configuration")) {
                report.add(new Finding(Severity.ERROR, "code", "removed-method",
                        "Removed in Flink 2.0: RichFunction.open(Configuration)",
                        "open(Configuration) override", where, line(md),
                        "Override open(OpenContext openContext) instead (org.apache.flink.api.common.functions.OpenContext).",
                        rules.group("member").link()));
            }
        }

        private void checkConfigKeyLiteral(StringLiteralExpr lit) {
            rules.removedConfigKey(lit.getValue()).ifPresent(k -> report.add(new Finding(
                    k.stillReferencedIn2x() ? Severity.INFO : Severity.WARNING,
                    "code",
                    "removed-config/" + k.key(),
                    k.stillReferencedIn2x()
                            ? "Config key listed as removed in 2.0, but 2.0 still recognises it"
                            : "Config key set in code was removed in Flink 2.0",
                    "\"" + k.key() + "\"",
                    where, line(lit),
                    rules.group("config").fix(),
                    rules.group("config").link())));
        }

        // ------------------------------------------------------------ reporting

        private void reportClassIfRemoved(String fqcn, int line) {
            rules.removedClass(fqcn).ifPresent(rc -> reportClass(rc, line));
        }

        private void reportClass(RemovedClass rc, int line) {
            if (!reportedClasses.add(rc.fqcn())) {
                return;
            }
            String groupName = rc.group();
            Rules.Group g = rules.group(groupName);
            if (rc.moved()) {
                report.add(new Finding(Severity.ERROR, "code", "moved-class", "Class moved to a new package in Flink 2.0",
                        rc.fqcn(), where, line, "Change the import to " + rc.movedTo() + ".", rules.group("other").link()));
                return;
            }
            String title = groupName.equals("connector")
                    ? "Class is gone from the Flink 2.x release of " + rc.origin()
                    : g.title();
            report.add(new Finding(Severity.ERROR, "code", "removed-class/" + groupName, title,
                    rc.fqcn(), where, line, g.fix(), g.link()));
        }

        private void reportPackage(String name, String group, int line) {
            if (!reportedClasses.add(name)) {
                return;
            }
            Rules.Group g = rules.group(group);
            report.add(new Finding(Severity.ERROR, "code", "removed-package/" + group, g.title(),
                    name, where, line, g.fix(), g.link()));
        }

        private void reportMember(RemovedMember m, int line, boolean resolvedReceiver) {
            Rules.Group g = rules.group("member");
            String owner = m.owner().substring(m.owner().lastIndexOf('.') + 1);
            String what = m.kind().equals("method") ? owner + "." + m.name() + "(...)" : owner + "." + m.name();
            report.add(new Finding(
                    resolvedReceiver ? Severity.ERROR : Severity.WARNING,
                    "code",
                    "removed-" + m.kind(),
                    resolvedReceiver
                            ? "Removed in Flink 2.0: " + what
                            : "Probably calls " + what + ", removed in Flink 2.0 (receiver type not resolved)",
                    m.owner() + ": " + m.signature(),
                    where, line, rules.memberHint(m.owner(), m.name()).orElse(g.fix()), g.link()));
        }

        private int line(Node n) {
            return n.getBegin().map(p -> p.line).orElse(0);
        }
    }

    // ---------------------------------------------------------------- Scala

    private void scanScala(Path root, Path file) {
        String where = ProjectFiles.display(root, file);
        List<String> lines;
        try {
            lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            report.add(info("code", "parse-error", "Could not read source file", e.getMessage(), where, 0));
            return;
        }
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < lines.size(); i++) {
            Matcher m = SCALA_IMPORT.matcher(lines.get(i));
            if (!m.find()) {
                continue;
            }
            String name = m.group(1).replaceAll("\\.$", "");
            int lineNo = i + 1;
            Optional<String> pkgGroup = rules.removedPackageGroup(name);
            if (pkgGroup.isPresent() && seen.add(name)) {
                Rules.Group g = rules.group(pkgGroup.get());
                report.add(new Finding(Severity.ERROR, "code", "removed-package/" + pkgGroup.get(), g.title(),
                        name, where, lineNo, g.fix(), g.link()));
                continue;
            }
            rules.removedClass(name).filter(rc -> seen.add(rc.fqcn())).ifPresent(rc -> {
                Rules.Group g = rules.group(rc.group());
                report.add(new Finding(Severity.ERROR, "code", "removed-class/" + rc.group(), g.title(),
                        rc.fqcn(), where, lineNo, g.fix(), g.link()));
            });
        }
    }

    private Finding info(String area, String id, String title, String detail, String where, int line) {
        return new Finding(Severity.INFO, area, id, title, detail, where, line, "", "");
    }
}
