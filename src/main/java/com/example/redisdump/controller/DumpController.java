package com.example.redisdump.controller;

import com.example.redisdump.service.DumpService;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
@Slf4j
@RestController
@RequestMapping("/api")
public class DumpController {

    private final DumpService dumpService;

    public DumpController(DumpService dumpService) {
        this.dumpService = dumpService;
    }

    @GetMapping("/dump")
    public ResponseEntity<String> triggerDump() {
        dumpService.dumpAll();
        return ResponseEntity.ok("Dump triggered");
    }
}