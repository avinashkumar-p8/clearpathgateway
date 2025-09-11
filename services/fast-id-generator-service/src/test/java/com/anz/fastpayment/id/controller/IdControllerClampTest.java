package com.anz.fastpayment.id.controller;

import com.anz.fastpayment.id.service.IdGeneratorService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

class IdControllerClampTest {

    private IdGeneratorService service;
    private IdController controller;

    @BeforeEach
    void setup() {
        service = mock(IdGeneratorService.class);
        controller = new IdController(service);
    }

    @Test
    void size_below_one_is_clamped_to_one() {
        when(service.nextPuidBlock("G31", 1)).thenReturn(Collections.singletonList("x"));
        controller.puidBlock("G31", 0);
        verify(service, times(1)).nextPuidBlock("G31", 1);
    }

    @Test
    void size_above_thousand_is_clamped_to_1000() {
        when(service.nextPuidBlock("G31", 1000)).thenReturn(Collections.singletonList("y"));
        controller.puidBlock("G31", 50_000);
        verify(service, times(1)).nextPuidBlock("G31", 1000);
    }
}
