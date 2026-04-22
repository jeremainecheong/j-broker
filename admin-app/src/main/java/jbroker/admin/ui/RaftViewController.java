package jbroker.admin.ui;

import jbroker.admin.api.RaftController;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * Thymeleaf view controller for the Raft state table. Reuses the JSON
 * endpoint's gather path so UI + REST can't drift.
 */
@Controller
public class RaftViewController {

    private final RaftController jsonController;

    public RaftViewController(RaftController jsonController) {
        this.jsonController = jsonController;
    }

    @GetMapping("/raft")
    public String raft(Model model) {
        model.addAttribute("nodes", jsonController.raftClusterState());
        return "raft";
    }
}
