<template>
    <div class="mb-3">
        <label for="lunasea-notification-target" class="form-label">{{ $t("Target") }}<span style="color: red;"><sup>*</sup></span></label>
        <div class="form-text">
            <p>
                <select id="lunasea-notification-target" v-model="$parent.notification.lunaseaTarget" class="form-select" required>
                    <option value="device">Device</option>
                    <option value="user">User</option>
                </select>
            </p>
        </div>
        <div v-if="$parent.notification.lunaseaTarget === 'device'">
            <label for="lunasea-device" class="form-label">{{ $t("Device ID") }}<span style="color: red;"><sup>*</sup></span></label>
            <input id="lunasea-device" v-model="$parent.notification.lunaseaDevice" type="text" class="form-control">
        </div>
        <div v-if="$parent.notification.lunaseaTarget === 'user'">
            <label for="lunasea-device" class="form-label">{{ $t("User ID") }}<span style="color: red;"><sup>*</sup></span></label>
            <input id="lunasea-device" v-model="$parent.notification.lunaseaUserID" type="text" class="form-control">
        </div>
    </div>
</template>

<script lang="ts">

export default {
    mounted() {
        if (typeof this.$parent.notification.lunaseaTarget === "undefined") {
            this.$parent.notification.lunaseaTarget = "device";
        }
    }
};

</script>
