package io.github.prajwaljadhav01.flink2check.scan;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

/** Walks a project tree, skipping build output and VCS folders. */
final class ProjectFiles {

    private static final Set<String> SKIP_DIRS = Set.of(
            ".git", ".idea", ".gradle", ".mvn", "target", "build", "out", "node_modules", ".venv", "venv");

    private ProjectFiles() { }

    static List<Path> find(Path root, Predicate<Path> matcher) {
        if (Files.isRegularFile(root)) {
            return matcher.test(root) ? List.of(root) : List.of();
        }
        try (Stream<Path> s = Files.walk(root)) {
            return s.filter(p -> !isSkipped(root, p))
                    .filter(Files::isRegularFile)
                    .filter(matcher)
                    .sorted()
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static boolean isSkipped(Path root, Path p) {
        Path rel = root.relativize(p);
        for (Path part : rel) {
            if (SKIP_DIRS.contains(part.toString())) {
                return true;
            }
        }
        return false;
    }

    static String display(Path root, Path file) {
        Path base = Files.isRegularFile(root) ? root.getParent() : root;
        return base == null ? file.toString() : base.relativize(file).toString();
    }

    static String nameOf(Path p) {
        return p.getFileName().toString();
    }
}
