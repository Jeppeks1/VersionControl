package main.java;

// Git - Rebase - ~2 - reword "fixed redundancy" fails

import main.java.configuration.Config;
import main.java.core.Core;

public class VersionControl {

    int value = 50;

    public static void main(String[] args) {
        String str = "This is the main function";

        Core core = new Core();
        Config config = new Config();

        System.out.println(core.str);
        System.out.println(config.config);
    }
}
