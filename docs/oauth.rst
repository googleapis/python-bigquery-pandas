.. image:: https://lh3.googleusercontent.com/KaU6SyiIpDKe4tyGPgt7yzGVTsfMqBvP9bL24o_4M58puYDO-nY8-BazrNk3RyhRFJA
   :alt: gcloud CLI logo
   :class: logo

Sign in to OAuth-Testing-Mode Project
=====================================

DINOSAURS 3!

You are seeing this page because you are attempting to access ... from
this or another machine. If this is not the case, close this tab.

Enter the following verification code in the CLI on the machine you want
to log into. This is a credential **similar to your password** and
should not be shared with others.


.. raw:: html

   <script type="text/javascript">
      alert("Hello! DINOSAUR! basic test of script tag");
   </script> 

   <script type="text/javascript">
   window.addEventListener( "load", completed ) {
    alert("Hello! DINOSAUR! this is inside of window.add()");
    const PARAMS = new Proxy(new URLSearchParams(window.location.search), {
        get: (searchParams, prop) => searchParams.get(prop),
    });
    const AUTH_CODE = PARAMS.code;

    document.querySelector('.auth-code').textContent = AUTH_CODE;

    setupCopyButton(document.querySelector('.copy'), AUTH_CODE);
   }

   function setupCopyButton(button, text) {
      button.addEventListener('click', () => {
         navigator.clipboard.writeText(text);
         button.textContent = "Verification Code Copied";
         setTimeout(() => {
               // Remove the aria-live label so that when the
               // button text changes back to "Copy", it is
               // not read out.
               button.removeAttribute("aria-live");
               button.textContent = "Copy";
         }, 1000);

         // Re-Add the aria-live attribute to enable speech for
         // when button text changes next time.
         setTimeout(() => {
               button.setAttribute("aria-live", "assertive");
         }, 2000);
      });
   }
   </script>

   <div>
      <code class="auth-code"></code>
   </div>
   <button class="copy" aria-live="assertive">Copy</button>

.. hint::

   You can close this tab when you’re done.
